//////////////////////////////////////////////////////////////////////
// Hanoi-style index combiner.
//
// To create a balance between reducing the number of files, and the
// number of times we have to merge files together, use a "Towers of
// Hanoi" type of algorithm to determine when to comtine the current
// index with previous files into a larger index file.  Numbering the
// index files from zero, the default rule is to not combine any
// values.  Then, if the (n%2)==1, consider combining with the
// previous index file.  If (n%4)==3, consider combining with the
// previous 2 index files, and so on.  Always use the rule that gives
// the largest combination.
//
// This object provides some simple utilities to make these decisions,
// and to help during recovery.

package org.davidb.jpool

object HanoiCombiner {

  // Determine how many entries to combine for a given index number.
  def combineCount(index: Int): Int = Integer.numberOfTrailingZeros(index + 1)

  // For a given index, return a set of index files that should be
  // present.  The index itself will always be in the list.  The
  // entries in the list will be in order.
  def presentSet(index: Int): List[Int] = {
    var s: List[Int] = Nil

    var i = index + 1
    while (i > 0) {
      s = (i - 1) :: s
      i = i & ~Integer.lowestOneBit(i)
    }

    s
  }
}
