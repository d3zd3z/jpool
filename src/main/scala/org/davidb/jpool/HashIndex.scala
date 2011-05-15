/**********************************************************************/
//
// The storage pool stores blobs in a series of files (the pool).  The
// HashIndex keeps track of where in this pool each hash is located.
//
// The HashIndex is stored as a series of memory-mapped files, sorted
// by Hash, containing the hash, and some "extra" information.  The
// number of files used is a tradeoff between the extra search time
// (all files have to be searched, as well as the kernel having to
// maintain extra mappings), and the time needed to rewrite the last
// file.
//
// At any given time, the HashIndex maintains 0 or more mappings of
// files as well as an internal update of hashes recently written that
// haven't been flushed out.  See the implementation details below on
// the Hanoi-style combination to give a decent tradeoff between
// individual files, and merges that need to be performed.
//
// The index is robust against abnormal termination of the program, as
// long as the underlying filesystem implements reasonable atomic
// semantics on rename.

package org.davidb.jpool

import collection.{mutable, immutable}

import java.io.{File, IOException}
import java.nio.ByteBuffer

import java.util.regex.Pattern
import java.util.Properties

trait HashIndex[E] extends mutable.Map[Hash, E] {
  protected val encoder: FixedEncodable[E]
  val basePath: String
  val prefix: String

  val properties = new Properties
  var dirtyProperties = false

  // Some argument sanity checks.
  if (!new File(basePath).isDirectory)
    throw new IllegalArgumentException("path does not specify a directory")
  if (prefix.indexOf('/') >= 0)
    throw new IllegalArgumentException("prefix must not contain a '/'")

  private var ramMap = immutable.TreeMap[Hash, E]()
  private var fileMap = new mutable.Stack[Option[MIF]]
  private var nextIndex = 0

  // User visible property accessors.
  def getProperty(pname: String): String = properties.getProperty(pname)
  def getProperty(pname: String, default: String): String = properties.getProperty(pname, default)
  def setProperty(pname: String, value: String) {
    properties.setProperty(pname, value)
    dirtyProperties = true
  }

  // Scan for index files based on the prefix, map the appropriate
  // files, and cleanup from any partial state we were in.
  private def indexScan {
    val unames = new File(basePath).listFiles()
    if (unames == null)
      throw new IOException("Unable to read contents of pool index dir: " + basePath)
    val names = util.Sorting.stableSort(unames, ((n: File) => n.getName))

    // Scan for straggling tmp files.
    val tmpRe = Pattern.compile("^" + Pattern.quote(prefix) + "\\d{4}\\.tmp$")
    for (n <- names; if tmpRe.matcher(n.getName).matches) {
        throw new Exception("Should delete tmp: " + n.getPath)
    }

    val indexRe = Pattern.compile("^" + Pattern.quote(prefix) + "(\\d{4})$")
    val iFiles = for (n <- names; m = indexRe.matcher(n.getName); if m.matches)
      yield (Integer.parseInt(m.group(1)) - 1)

    if (iFiles.size > 0) {
      // An untimely powerdown may have left some files present that
      // have already been merged.  Use the information in the last
      // index file to determine where we left off.

      val lastIndex = iFiles.last
      val tip = newMIF(lastIndex)
      tip.mmap()
      val combineText = properties.getProperty("index.lastCombine")
      if (combineText == null)
        sys.error("Pool file: " + mkpath(lastIndex) +
          " doesn't contain proper index.lastCombine property")
      val lastCombine = Integer.parseInt(combineText) - 1

      if (lastCombine < lastIndex - 1 || lastCombine > lastIndex)
        sys.error("Pool file: " + mkpath(lastIndex) +
          " has out of bounds index.lastCombine property")

      // If there is an index file past the last combination, then the
      // last index is partial.
      val partialLastIndex = lastCombine < lastIndex

      // Track all expected files.
      var expectedFiles = immutable.TreeSet[Int]()
      expectedFiles ++= HanoiCombiner.presentSet(lastCombine)

      // Make sure the last index is always expected so it isn't
      // deleted.
      expectedFiles += lastIndex

      purgeExtraNames(iFiles, expectedFiles)

      // Open each expected file.
      for (i <- expectedFiles) {
        val mif = if (i == lastIndex) tip else {
          // Special case.  These do not get the properties.
          val mif = new MIF(mkpath(i))
          mif.mmap()
          mif
        }
        // println("Adding mif: " + mif.path)
        // for ((k, e) <- mif) {
        //   print(" " + k + " " + e.toString)
        // }
        // println()
        fileMap.push(Some(mif))
      }
      nextIndex = lastIndex

      if (!partialLastIndex) {
        fileMap.push(None)
        nextIndex += 1
      }
    } else {
      // No existing files.
      fileMap.push(None)
      nextIndex = 0
    }
  }

  // Note that this constructor will run <b>before</b> the subclass
  // constructor, and since this uses the encoder, it is important
  // that this be initialized.  Several on the scala-user mailing list
  // suggested making the 'encoder' a lazy value which allows it's
  // value to be computed before the subclass constructor itself has
  // been run, and this seems to work.
  indexScan

  // Purge any filenames that should have been deleted, and fail if
  // there are any that should be present that aren't.
  private def purgeExtraNames(found: Iterable[Int], expected: Iterable[Int]) {
    // println("found: " + found.toList)
    // println("expec: " + expected.toList)

    val f = found.iterator.buffered
    val e = expected.iterator.buffered

    while (f.hasNext && e.hasNext) {
      if (f.head < e.head)
        sys.error("TODO: Found extra file: " + mkpath(f.head))
      else if (e.head < f.head)
        sys.error("TODO: File missing: " + mkpath(e.head))
      else {
        f.next
        e.next
      }
    }

    if (f.hasNext) {
      f.next
      sys.error("TODO: Found extra file: " + mkpath(f.head))
    }
    if (e.hasNext) {
      e.next
      sys.error("TODO: File missing: " + mkpath(e.head))
    }
  }

  def flush() {
    if ((ramMap ne null) && (ramMap.size > 0 || dirtyProperties))
      combine(0, false)
  }
  def close() {
    flush()
    ramMap = null
    fileMap = null
  }

  // Maximum number of hash entries in the RAM cached entry.  Larger
  // values will take more memory.  The RAM implementation uses a
  // TreeMap which is several times larger than the compact mapped
  // representation.  The tradeoff is that TreeMap insertions are
  // relatively fast, whereas adding to the mapped version requires
  // merging and rewriting files.  Every time <code>ramMax</code>
  // entries are added, a new index file will be created and/or merged
  // with previous results.
  def ramMax = 37000

  private class MIF(val path: File) extends MappedIndexFile[E] {
    protected val encoder = HashIndex.this.encoder
  }
  private def newMIF(index: Int): MIF = {
    val mif = new MIF(mkpath(index))
    mif.properties = properties
    mif
  }

  // Number of index nodes in the tip.
  private def getTipSize: Int = {
    val base = ramMap.size
    fileMap.top match {
      case None => base
      case Some(t) => base + t.size
    }
  }

  // Combine any previous index files, the current index file, and the
  // RAM index.
  private def mergeFiles() {
    // println("Merging ram " + ramMap.size + " nodes, max = " + ramMax)
    val cc = HanoiCombiner.combineCount(nextIndex)
    combine(cc, true)
    nextIndex += 1
    fileMap.push(None)
  }

  private def combine(cc: Int, bumping: Boolean) {
    val iter = new MergingMapIterator[Hash, E]
    iter.addIterator(ramMap)

    val removals = new mutable.ListBuffer[MIF]()

    val tip = fileMap.pop match {
      case None => newMIF(nextIndex)
      case Some(tip) =>
        iter.addIterator(tip)
        tip
    }

    for (i <- 0 until cc) {
      val node = fileMap.pop() match {
        case Some(tip) => tip
        case None => sys.error("Internal error")
      }
      removals += node
      iter.addIterator(node)
    }

    val lastCombine = nextIndex - (if (bumping) 0 else 1)

    properties.setProperty("version", "1.0")
    properties.setProperty("index.lastCombine", (lastCombine + 1).toString)

    tip.properties = properties
    tip.write(iter)

    for (r <- removals) {
      r.path.delete()
    }

    fileMap.push(Some(tip))
    ramMap = immutable.TreeMap[Hash, E]()
    dirtyProperties = false
  }

  private def mkpath(index: Int): File = {
    new File(String.format("%s/%s%04d", basePath, prefix, int2Integer(index + 1)))
  }

  // mutable.Map implementations.
  def -= (key: Hash) = throw new UnsupportedOperationException("HashIndex can only be added to")
  def += (kv: (Hash, E)): this.type = {
    val (key, value) = kv
    get(key) match {
      case Some(v2) =>
        if (value != v2)
          throw new UnsupportedOperationException("HashIndex update cannot modify existing values")
      case None =>
        ramMap += (key -> value)
        if (getTipSize >= ramMax)
          mergeFiles()
    }
    this
  }
  def get(key: Hash): Option[E] = {
    ramMap.get(key) match {
      case result @ Some(_) => result
      case None => getMapped(key, fileMap.toList)
    }
  }
  private def getMapped(key: Hash, fm: List[Option[MIF]]): Option[E] = {
    fm match {
      case Nil => None
      case None::r => getMapped(key, r)
      case Some(m)::r =>
        m.get(key) match {
          case result @ Some(_) => result
          case None => getMapped(key, r)
        }
    }
  }

  def iterator: Iterator[(Hash, E)] = sys.error("TODO")
}
