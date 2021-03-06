{-# LANGUAGE MultiParamTypeClasses, RankNTypes, ScopedTypeVariables #-}

{- |
   Module      : Streaming.Sort
   Description : Sorting values in a Stream
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com


   This module is designed to be imported qualified.

 -}
module Streaming.Sort
  ( -- * In-memory sorting
    -- $memory
    sort
  , sortBy
  , sortOn
    -- * File-based sorting
    -- $filesort
  , withFileSort
  , withFileSortBy
    -- ** Exceptions
  , SortException (..)
    -- ** Configuration
  , Config
  , defaultConfig
    -- $lenses
  , setConfig
  , chunkSize
  , maxFiles
  , useDirectory
  )  where

import           Streaming         (Of(..), Stream)
import qualified Streaming         as S
import           Streaming.Binary  (decoded)
import qualified Streaming.Prelude as S
import           Streaming.With

import           Data.Binary               (Binary, encode)
import qualified Data.ByteString.Lazy      as BL
import qualified Data.ByteString.Streaming as BS

import           Control.Exception         (Exception(..), IOException,
                                            mapException)
import           Control.Monad             (join, void)
import           Control.Monad.Catch       (MonadMask, MonadThrow, finally,
                                            throwM)
import           Control.Monad.IO.Class    (MonadIO, liftIO)
import           Control.Monad.Trans.Class (lift)
import           Data.Bool                 (bool)
import           Data.Coerce               (Coercible, coerce)
import           Data.Function             (on)
import           Data.Functor.Identity     (Identity(Identity), runIdentity)
import           Data.Int                  (Int64)
import qualified Data.List                 as L
import           Data.Maybe                (catMaybes)
import           System.Directory          (doesDirectoryExist, getPermissions,
                                            removeFile, writable)
import           System.IO                 (hClose, openBinaryTempFile)

--------------------------------------------------------------------------------

{- $memory

These functions require reading all the values from the Stream before
being able to sort them.  As such, it is /highly/ recommended you only
use them for short Streams.

-}

-- | Sort the values based upon their 'Ord' instance.
sort :: (Monad m, Ord a) => Stream (Of a) m r -> Stream (Of a) m r
sort = sortBy compare

-- | Use the specified comparison function to sort the values.
sortBy :: (Monad m) => (a -> a -> Ordering) -> Stream (Of a) m r -> Stream (Of a) m r
sortBy cmp s = lift (S.toList s) >>= srt
  where
    srt (as :> r) = S.each (L.sortBy cmp as) >> return r

-- | Use the provided function to be able to compare values.
sortOn :: (Ord b, Monad m) => (a -> b) -> Stream (Of a) m r -> Stream (Of a) m r
sortOn f = S.map fst
           . sortBy (compare `on` snd)
           . S.map ((,) <*> f)

--------------------------------------------------------------------------------

{- $filesort

For large Streams it may not be possible to sort it entirely in
memory.  As such, these functions work by sorting chunks of the Stream
and storing them in temporary files before merging them all together.

These functions may throw a 'SortException'.

-}

data Config = Config
  { _chunkSize    :: !Int
    -- ^ Size of chunks to sort in-memory.
  , _maxFiles     :: !Int
    -- ^ The maximum number of temporary files to be reading at any
    -- one time.
  , _useDirectory :: !(Maybe FilePath)
    -- ^ Where to store temporary files.  Will be cleaned up
    -- afterwards.  'Nothing' indicates to use the system temporary
    -- directory.
  } deriving (Show)

-- | Default settings for sorting using external files:
--
--   * Have a chunk size of @1000@.
--
--   * No more than @100@ temporary files to be open at a time.
--
--   * Use the system temporary directory.
defaultConfig :: Config
defaultConfig = Config
  { _chunkSize    = 1000
  , _maxFiles     = 100
  , _useDirectory = Nothing
  }

{- $lenses

These functions are lenses compatible with the @lens@, @microlens@,
etc. libraries.

-}

-- type Lens s t a b = forall f. (Functor f) => (a -> f b) -> s -> f t

-- | A specialised variant of @set@ from lens, microlens, etc. defined
--   here in case you're not using one of those libraries.
setConfig :: (forall f. (Functor f) => (a -> f a) -> Config -> f Config)
             -> a -> Config -> Config
setConfig lens a = runIdentity #. lens (const (Identity a))
{-# INLINABLE setConfig #-}

(#.) :: (Coercible c b) => (b -> c) -> (a -> b) -> (a -> c)
(#.) _ = coerce (\x -> x :: b) :: forall a b. Coercible b a => a -> b

chunkSize :: (Functor f) => (Int -> f Int) -> Config -> f Config
chunkSize inj cfg = (\v -> cfg { _chunkSize = v}) <$> inj (_chunkSize cfg)
{-# INLINABLE chunkSize #-}

maxFiles :: (Functor f) => (Int -> f Int) -> Config -> f Config
maxFiles inj cfg = (\v -> cfg { _maxFiles = v}) <$> inj (_maxFiles cfg)
{-# INLINABLE maxFiles #-}

useDirectory :: (Functor f) => (Maybe FilePath -> f (Maybe FilePath)) -> Config -> f Config
useDirectory inj cfg = (\v -> cfg { _useDirectory = v}) <$> inj (_useDirectory cfg)
{-# INLINABLE useDirectory #-}

--------------------------------------------------------------------------------

-- | Use external files to temporarily store partially sorted (using
--   the comparison function) results (splitting into chunks of the
--   specified size if one is provided).
--
--   These files are stored inside the specified directory if
--   provided; if no such directory is provided then the system
--   temporary directory is used.
withFileSort :: (Ord a, Binary a, MonadMask m, MonadIO m, MonadThrow n, MonadIO n)
                => Config -> Stream (Of a) m v
                -> (Stream (Of a) n () -> m r) -> m r
withFileSort cfg = withFileSortBy cfg compare

-- | Use external files to temporarily store partially sorted (using
--   the comparison function) results (splitting into chunks of the
--   specified size if one is provided).
--
--   These files are stored inside the specified directory if
--   provided; if no such directory is provided then the system
--   temporary directory is used.
withFileSortBy :: (Binary a, MonadMask m, MonadIO m, MonadThrow n, MonadIO n)
                  => Config -> (a -> a -> Ordering) -> Stream (Of a) m v
                  -> (Stream (Of a) n () -> m r) -> m r
withFileSortBy cfg cmp str k = mapException SortIO . createDir $ \dir ->
  mergeAllFiles (_maxFiles cfg) dir cmp (initStream dir) k
  where
    createDir k' = liftIO (traverse checkDir (_useDirectory cfg)) -- :: m (Maybe (Maybe FilePath))
                   -- If a directory is specified, make sure we can actually use it
                   >>= (`getTmpDir` k') . join -- join is to get rid of double Maybe

    -- Make sure the directory exists and is writable.
    checkDir dir = do exists <- doesDirectoryExist dir
                      canWrite <- writable <$> getPermissions dir
                      return (bool Nothing (Just dir) (exists && canWrite))

    getTmpDir mdir = maybe withSystemTempDirectory withTempDirectory mdir "streaming-sort"

    initStream dir = initialSort (_chunkSize cfg) dir cmp str

-- | Do initial in-memory sorting, writing the results to disk.
initialSort :: (Binary a, MonadMask m, MonadIO m)
               => Int -> FilePath -> (a -> a -> Ordering)
               -> Stream (Of a) m r
               -> Stream (Of FilePath) m r
initialSort chnkSize dir cmp =
  S.mapped (writeSortedData dir)
  . S.maps (sortBy cmp)
  . S.chunksOf chnkSize

-- | Recursively merge files containing sorted data.
mergeAllFiles :: (Binary a, MonadMask m, MonadIO m, MonadThrow n, MonadIO n)
                 => Int -> FilePath -> (a -> a -> Ordering)
                 -> Stream (Of FilePath) m v
                 -> (Stream (Of a) n () -> m r) -> m r
mergeAllFiles numFiles tmpDir cmp files k = go files
  where
    go = checkEmpty . S.mapped S.toList . S.chunksOf numFiles

    -- If no files, then evaluate the continuation with the empty Stream.
    checkEmpty chunks = S.uncons chunks >>= maybe (k (return ()))
                                                  (uncurry checkSingleChunk)

    -- In a single chunk there's no need to write the contents back
    -- out to disk.
    checkSingleChunk ch chunks =
      S.uncons chunks >>= maybe (withFilesSort cmp ch k)
                                (uncurry (withMultipleChunks ch))

    withMultipleChunks ch1 ch2 chunks = go (S.mapM sortAndWrite allChunks)
      where
        allChunks = S.yield ch1 >> S.yield ch2 >> chunks

        sortAndWrite fls = withFilesSort cmp fls (fmap S.fst' . writeSortedData tmpDir)

-- | Encode and write data to a new temporary file located within the
--   specified directory.
--
--   (Data not actually required to be sorted.)
writeSortedData :: (Binary a, MonadMask m, MonadIO m)
                   => FilePath -> Stream (Of a) m r -> m (Of FilePath r)
writeSortedData tmpDir str = do fl <- liftIO newTmpFile
                                (fl :>) <$> writeBinaryFile fl (encodeStream str)
  where
    newTmpFile = do (fl, h) <- openBinaryTempFile tmpDir "streaming-sort-chunk"
                    hClose h -- Just want a new filename, not the actual Handle
                    return fl

-- | Read in the specified files and merge the sorted streams.
withFilesSort :: (Binary a, MonadMask m, MonadIO m, MonadThrow n, MonadIO n)
                 => (a -> a -> Ordering) -> [FilePath]
                 -> (Stream (Of a) n () -> m r) -> m r
withFilesSort cmp files k = mergeContinuations readThenDelete
                                               files
                                               withMerged
  where
    withMerged = k . interleave cmp . map decodeStream

-- | A wrapper around 'withBinaryFileContents' that deletes the file
--   afterwards.
readThenDelete :: (MonadMask m, MonadIO m, MonadIO n) => FilePath
                  -> (BS.ByteString n () -> m r) -> m r
readThenDelete fl k = withBinaryFileContents fl k `finally` liftIO (removeFile fl)

-- | Fold a list of continuations into one overall continuation.
mergeContinuations :: (Monad m) => (forall res. a -> (b -> m res) -> m res) -> [a] -> ([b] -> m r) -> m r
mergeContinuations toCont xs cont = go [] xs
  where
    go bs []     = cont bs
    go bs (a:as) = toCont a $ \b -> go (b:bs) as

-- | Merge multiple streams together using the provided comparison
--   function.  Assumes provided streams are already sorted.
interleave :: (Monad m) => (a -> a -> Ordering) -> [Stream (Of a) m r] -> Stream (Of a) m ()
interleave cmp streams =
  go =<< lift (L.sortBy cmper . catMaybes <$> mapM S.uncons streams)
  where
    -- We take as input a list of @(a, Stream)@ tuples sorted on the @a@
    -- value to represent non-empty Streams.
    go []               = return ()
    go [(a,str)]        = S.yield a >> void str -- Only stream left!
    go ((a,str):astrs') = do S.yield a
                             -- Try to get the next element
                             mastr' <- lift (S.uncons str)
                             go (addBackIfNonEmpty mastr' astrs')

    -- If a Stream is non-empty, place it back in the correct position.
    addBackIfNonEmpty = maybe id (L.insertBy cmper)

    cmper = cmp `on` fst

-- | Streaming.Binary.encoded uses Builder under the hood, requiring IO.
encodeStream :: (Binary a, Monad m) => Stream (Of a) m r -> BS.ByteString m r
encodeStream = fromChunksLazy . S.map encode

-- | A wrapper around 'decoded' that throws an exception rather than
--   returning failure.
decodeStream :: (Binary a, MonadThrow m) => BS.ByteString m r -> Stream (Of a) m r
decodeStream bs = decoded bs >>= handleResult
  where
    handleResult (_, bytes, res) = either (lift . throwM . SortDecode bytes) return res

fromChunksLazy :: (Monad m) => Stream (Of BL.ByteString) m r -> BS.ByteString m r
fromChunksLazy = BS.fromChunks . S.concat . S.map BL.toChunks

--------------------------------------------------------------------------------

data SortException = SortIO IOException
                   | SortDecode Int64 String
  deriving (Show)

instance Exception SortException
