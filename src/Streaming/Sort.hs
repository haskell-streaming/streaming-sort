{-# LANGUAGE RankNTypes #-}

{- |
   Module      : Streaming.Sort
   Description : Sorting values in a Stream
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com


   This module is designed to be imported qualified.

 -}
module Streaming.Sort (
    -- * In-memory sorting
    -- $memory
    sort
  , sortBy
  , sortOn
    -- * File-based sorting
    -- $filesort
  -- , spfilesort
  -- , spfilesortBy
    -- ** Configuration
  , Config , defaultConfig
    -- $lenses
  , chunkSize
  , maxFiles
  , tmpDir
  )  where

import           Streaming         (Of, Stream)
import           Streaming.Binary  (decoded)
import qualified Streaming.Prelude as S
import           Streaming.With

import           Data.Binary               (Binary, encode)
import qualified Data.ByteString.Lazy      as BL
import qualified Data.ByteString.Streaming as BS

import           Control.Monad             (void)
import           Control.Monad.Catch       (MonadMask, bracket)
import           Control.Monad.IO.Class    (MonadIO, liftIO)
import           Control.Monad.Trans.Class (lift)
import           Data.Function             (on)
import qualified Data.List                 as L
import           Data.Maybe                (catMaybes)

--------------------------------------------------------------------------------

{- $memory

These functions require reading all the values from the Stream before
being able to sort them.  As such, it is /highly/ recommended you only
use them for short Streams.

-}

-- | Sort the values based upon their 'Ord' instance.
sort :: (Monad m, Ord a) => Stream (Of a) m r -> Stream (Of a) m ()
sort = sortBy compare

-- | Use the specified comparison function to sort the values.
sortBy :: (Monad m) => (a -> a -> Ordering) -> Stream (Of a) m r -> Stream (Of a) m ()
sortBy f s = S.each . L.sortBy f . S.fst' =<< lift (S.toList s)

-- | Use the provided function to be able to compare values.
sortOn :: (Ord b, Monad m) => (a -> b) -> Stream (Of a) m r -> Stream (Of a) m ()
sortOn f = S.map fst
           . sortBy (compare `on` snd)
           . S.map ((,) <*> f)

--------------------------------------------------------------------------------

{- $filesort

For large Streams it may not be possible to sort it entirely in
memory.  As such, these functions work by sorting chunks of the Stream
and storing them in temporary files before merging them all together.

-}

data Config = Config
  { _chunkSize :: !Int
    -- ^ Size of chunks to sort in-memory.
  , _maxFiles  :: !Int
    -- ^ The maximum number of temporary files to be reading at any
    -- one time.
  , _tmpDir    :: !(Maybe FilePath)
    -- ^ Where to store temporary files.  Will be cleaned up
    -- afterwards.  'Nothing' indicates to use the system temporary
    -- directory.
  }

-- | Default settings for sorting using external files:
--
--   * Have a chunk size of @1000@.
--
--   * No more than @100@ temporary files to be open at a time.
--
--   * Use the system temporary directory.
defaultConfig :: Config
defaultConfig  = Config
  { _chunkSize = 1000
  , _maxFiles  = 100
  , _tmpDir    = Nothing
  }

{- $lenses

These functions are lenses compatible with the @lens@, @microlens@,
etc. libraries.

-}

-- type Lens s t a b = forall f. (Functor f) => (a -> f b) -> s -> f t

chunkSize :: (Functor f) => (Int -> f Int) -> Config -> f Config
chunkSize inj cfg = (\v -> cfg { _chunkSize = v}) <$> inj (_chunkSize cfg)
{-# INLINABLE chunkSize #-}

maxFiles :: (Functor f) => (Int -> f Int) -> Config -> f Config
maxFiles inj cfg = (\v -> cfg { _maxFiles = v}) <$> inj (_maxFiles cfg)
{-# INLINABLE maxFiles #-}

tmpDir :: (Functor f) => (Maybe FilePath -> f (Maybe FilePath)) -> Config -> f Config
tmpDir inj cfg = (\v -> cfg { _tmpDir = v}) <$> inj (_tmpDir cfg)
{-# INLINABLE tmpDir #-}

--------------------------------------------------------------------------------

-- Need to handle exceptions from decoded

withFilesSort :: (Binary a, MonadMask m, MonadIO m, MonadIO n) => (a -> a -> Ordering) -> [FilePath]
                 -> (Stream (Of a) n () -> m r) -> m r
withFilesSort cmp fls cont = mergeContinuations withBinaryFileContents fls (cont . interleave cmp . map decoded)

mergeContinuations :: (Monad m) => (forall res. a -> (b -> m res) -> m res) -> [a] -> ([b] -> m r) -> m r
mergeContinuations toCont as cont = go [] as
  where
    go bs []     = cont bs
    go bs (a:as) = toCont a $ \b -> go (b:bs) as

interleave :: (Monad m) => (a -> a -> Ordering) -> [Stream (Of a) m r] -> Stream (Of a) m ()
interleave cmp streams =
  go =<< lift (catMaybes <$> mapM S.uncons streams)
  where
    go [] = return ()
    go [(a,str)] = S.yield a >> (void str)
    go astrs = do let (a,str):astrs' = L.sortBy (cmp`on`fst) astrs
                  S.yield a
                  mastr' <- lift (S.uncons str)
                  go ((maybe id (:) mastr') astrs')

-- | Streaming.Binary.encoded uses Builder under the hood, requiring IO.
encoded :: (Binary a, Monad m) => Stream (Of a) m r -> BS.ByteString m r
encoded = fromChunksLazy . S.map encode

fromChunksLazy :: (Monad m) => Stream (Of BL.ByteString) m r -> BS.ByteString m r
fromChunksLazy = BS.fromChunks . S.concat . S.map BL.toChunks
