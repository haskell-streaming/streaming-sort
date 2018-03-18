{-# LANGUAGE FlexibleContexts #-}

{- |
   Module      : Streaming.Sort.Lifted
   Description : Lifted variants of "Streaming.Sort"
   Copyright   : Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Streaming.Sort.Lifted
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
  ) where

import           Streaming.Sort (Config, SortException(..), chunkSize,
                                 defaultConfig, maxFiles, setConfig, sort,
                                 sortBy, sortOn, useDirectory)
import qualified Streaming.Sort as SS

import Control.Monad.Catch    (MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Data.Binary            (Binary)
import Streaming.Prelude      (Of, Stream)
import Streaming.With.Lifted  (Withable(WithMonad), liftWith)

--------------------------------------------------------------------------------

{- $memory

These functions require reading all the values from the Stream before
being able to sort them.  As such, it is /highly/ recommended you only
use them for short Streams.

-}

{- $filesort

For large Streams it may not be possible to sort it entirely in
memory.  As such, these functions work by sorting chunks of the Stream
and storing them in temporary files before merging them all together.

These functions may throw a 'SortException'.

-}

-- | Use external files to temporarily store partially sorted (using
--   the comparison function) results (splitting into chunks of the
--   specified size if one is provided).
--
--   These files are stored inside the specified directory if
--   provided; if no such directory is provided then the system
--   temporary directory is used.
withFileSort :: (Ord a, Binary a
                , Withable w, MonadMask (WithMonad w), MonadIO (WithMonad w)
                , MonadThrow m, MonadIO m)
                => Config -> Stream (Of a) (WithMonad w) v
                -> w (Stream (Of a) m ())
withFileSort cfg str = liftWith (SS.withFileSort cfg str)

-- | Use external files to temporarily store partially sorted (using
--   the comparison function) results (splitting into chunks of the
--   specified size if one is provided).
--
--   These files are stored inside the specified directory if
--   provided; if no such directory is provided then the system
--   temporary directory is used.
withFileSortBy :: (Ord a, Binary a, Withable w
                  , MonadMask (WithMonad w), MonadIO (WithMonad w)
                  , MonadThrow m, MonadIO m)
                  => Config -> (a -> a -> Ordering)
                  -> Stream (Of a) (WithMonad w) v
                  -> w (Stream (Of a) m ())
withFileSortBy cfg cmp str = liftWith (SS.withFileSortBy cfg cmp str)
