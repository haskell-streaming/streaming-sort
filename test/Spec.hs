{-# LANGUAGE RankNTypes #-}
{- |
   Module      : Main
   Description : Tests for sorting
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Main (main) where

import Streaming.Sort

import           Streaming.Prelude (Of, Stream)
import qualified Streaming.Prelude as S
import           Streaming.With    (withSystemTempDirectory)

import Control.Applicative       (liftA2)
import Control.Monad.Catch       (catchAll, throwM)
import Control.Monad.Trans.Class (lift)
import Data.Binary               (Binary)
import Data.Functor.Identity     (runIdentity)
import System.Directory          (getDirectoryContents)

import qualified Data.List as L

import Test.Hspec            (Expectation, describe, hspec, it, shouldReturn)
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck       (Gen, Positive(Positive), arbitrary, forAllShrink,
                              ioProperty, shrink)

--------------------------------------------------------------------------------

main :: IO ()
main = hspec $ do
  describe "in-memory" $
    prop "same as list-based" $
      forAllShrink (arbitrary :: Gen [Int]) shrink $
        liftA2 (==) L.sort (runIdentity . S.toList_ . sort . S.each)
  describe "file-based" $ do
    prop "same as list-based" $
      forAllShrink (arbitrary :: Gen [Int]) shrink $ \as (Positive cs) ->
        ioProperty $ withFileSort (setConfig chunkSize cs defaultConfig)
                                  (S.each as)
                                  (fmap (== L.sort as) . S.toList_)
    describe "cleans up temporary files" $ do
      it "on success" $
        fileSortCleanup (S.each [1::Int .. 10])
      it "on exception" $
        fileSortCleanup (S.each [1::Int .. 10] >> lift (throwM (userError "Tainted Producer")))

fileSortCleanup :: (Binary a, Ord a) => Stream (Of a) IO r -> Expectation
fileSortCleanup str = withSystemTempDirectory "test-streaming-sort-cleanup" $ \tmpDir -> do
  cnts0 <- getDirectoryContents tmpDir -- Should be [".", ".."]
  catchAll (withFileSort defaultConfig str S.effects) (const (return ()))
  (L.sort <$> getDirectoryContents tmpDir) `shouldReturn` L.sort cnts0
