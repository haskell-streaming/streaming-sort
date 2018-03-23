# Revision history for streaming-sort

## 0.1.0.2 -- 2018-03-23

* Remove extraneous constraints in `Streaming.Sort.Lifted` (`Withable`
  implies `MonadMask` and `MonadIO`).

## 0.1.0.1 -- 2018-03-23

* Remove accidental `Ord a` constraint on `withFileSortBy` in
  `Streaming.Sort.Lifted`.

## 0.1.0.0 -- 2018-03-18

* First version. Released on an unsuspecting world.
