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
  )  where

import Streaming         (Of, Stream)
import Streaming.Prelude as S

import           Control.Monad.Trans.Class (lift)
import           Data.Function             (on)
import qualified Data.List                 as L

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
