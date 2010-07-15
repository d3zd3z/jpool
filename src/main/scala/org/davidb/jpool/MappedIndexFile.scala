/*
 * Mapped Index Files.
 *
 * The workhorse of the HashIndex class.
 *
 * The mapped index maintains a fixed mapping between Hashes and
 * some type that can be encoded in a fixed binary representation.
 */

package org.davidb.jpool

import scala.annotation.tailrec
import java.io.{ FileOutputStream, BufferedOutputStream, DataOutputStream,
  FileInputStream, DataInputStream,
  ByteArrayOutputStream, ByteArrayInputStream }

import java.util.Properties
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.io.File

import collection.{mutable, immutable}

// Indexmaps are non-variant maps of Hashes to something else.
trait HashMap[E] extends collection.Map[Hash, E]

// TODO: Move this to it's own source file.
// TODO: The constraints aren't right, addIterator should be more
// flexible.
class MergingMapIterator[K <% Ordered[K], E] extends Iterable[(K, E)] {
  private val children = new mutable.ArrayBuffer[Iterable[(K, E)]]

  def addIterator(iter: Iterable[(K, E)]): MergingMapIterator[K, E] = {
    children += iter
    this
  }

  def iterator: Iterator[(K, E)] = new Iterator[(K, E)] {
    private val elems = children.toArray.map((e: Iterable[(K, E)]) => e.iterator)
    private val heads = new Array[Option[(K, E)]](elems.length)

    private def advance(i: Int) {
      if (elems(i).hasNext)
        heads(i) = Some(elems(i).next)
      else
        heads(i) = None
    }

    for (i <- 0 until elems.length) advance(i)

    def hasNext: Boolean = heads exists (_ != None)
    def next(): (K, E) = {
      var min = -1
      var mvalue: Option[(K, E)] = None
      for (i <- 0 until elems.length) {
        (mvalue, heads(i)) match {
          case (None, e) =>
            min = i
            mvalue = e
          case (Some((a,_)), e @ Some((b,_))) if (b < a) =>
            min = i
            mvalue = e
          case _ =>
        }
      }

      mvalue match {
        case Some(v) =>
          advance(min)
          v
        case _ => throw new NoSuchElementException("next on empty iterator")
      }
    }
  }
}

// Instances of FixedEncodable represent things that can be encoded.
// Instances are likely to all be concrete.
trait FixedEncodable[E] {
  def EBytes: Int
  def encode(obj: E, buf: ByteBuffer)
  def decode(buf: ByteBuffer): E
}

trait MappedIndexFile[E] extends HashMap[E] {
  mif =>

  protected val encoder: FixedEncodable[E]

  val path: File

  // The properties are publically visible, and can be modified.  It
  // is not used by the MappedIndexFile at all, but it's contents will
  // be written out upon write, and any in the file will be merged in.
  var properties = new Properties

  // Note: Depending on initialization order, the EBytes may not be
  // initialized, so this has to be a method rather than just a value.
  private def HashSpan: Int = Hash.HashLength + encoder.EBytes

  // Write the map represented by the iterator out to the file.  The
  // map is written to a temporary file, so that the current iterator
  // can be used as a source.  The iterator <b>must</b> visit the
  // hashes in properly sorted order, or the resulting index file will
  // not work correctly.
  // TODO: The writing technique seems overly complicated.
  def write(items: Iterable[(Hash, E)]) = {
    val tmpName = new File(path.getPath + ".tmp")
    val fos = new FileOutputStream(tmpName)
    val bos = new BufferedOutputStream(fos)
    val dos = new DataOutputStream(bos)

    storeProperties(dos)
    for ((h, e) <- items.elements) {
      dos.write(h.getBytes)
      val buf = new Array[Byte](encoder.EBytes)
      encoder.encode(e, ByteBuffer.wrap(buf))
      dos.write(buf)
    }

    bos.flush
    fos.getChannel.force(true)
    dos.close
    if (!tmpName.renameTo(path))
      error("Unable to rename pool index file")
    mmap()
  }

  // Establish the mapping with the file.  Loads the properties in,
  // and mmaps the data.
  def mmap() = {
    val fis = new FileInputStream(path)
    loadProperties(new DataInputStream(fis))

    val chan = fis.getChannel()
    val pos = chan.position
    val buf = chan.map(FileChannel.MapMode.READ_ONLY, pos, chan.size - pos)
    buf.load()
    assert(((chan.size - pos) % HashSpan) == 0)
    _size = (chan.size - pos).toInt / HashSpan
    mapped = buf
    chan.close()
  }

  private def storeProperties(dos: DataOutputStream) {
    val baos = new ByteArrayOutputStream
    properties.store(baos, "JPool index file")
    val data = baos.toByteArray()
    dos.writeInt(data.length)
    dos.write(data)

    val offset = 4 + data.length
    val pad = (-offset & 4095)
    if (pad > 0) {
      val padding = new Array[Byte](pad)
      dos.write(padding)
    }
  }

  private def loadProperties(dis: DataInputStream) {
    val len = dis.readInt()
    if (len <= 0 || len > 32768) // 32k is arbitrary, but hopefully large enough.
      error("Property index seems corrupt")
    val data = new Array[Byte](len)
    dis.readFully(data)

    val offset = 4 + len
    val pad = (-offset) & 4095
    if (pad > 0) {
      val padding = new Array[Byte](pad)
      dis.readFully(padding)
    }

    properties.load(new ByteArrayInputStream(data))
  }

  // Map interface.
  var mapped: ByteBuffer = null
  override def size: Int = _size
  var _size: Int = 0

  // These can't be updated.
  def + [B >: E](kv: (Hash, B)): HashMap[B] = error("Cannot update")
  def - (key: Hash): HashMap[E] = error("Cannot remove from hashmap")

  // Get is a simple binary search.
  def get(key: Hash): Option[E] = {
    val tmp = new Array[Byte](Hash.HashLength)
    @tailrec def loop(low: Int, high: Int): Option[E] = {
      if (low <= high) {
        val mid = (low + high) >>> 1;

        mapped.position(mid * HashSpan)
        mapped.get(tmp)
        val midHash = Hash.raw(tmp)
        val comp = midHash.compareTo(key)
        if (comp < 0)
          loop(mid + 1, high)
        else if (comp > 0)
          loop(low, mid - 1)
        else {
          // Found, get the element.
          val eBuf = new Array[Byte](encoder.EBytes)
          mapped.get(eBuf)
          Some(encoder.decode(ByteBuffer.wrap(eBuf)))
        }
      } else None
    }
    loop(0, size - 1)
  }

  def iterator: Iterator[(Hash, E)] = new Iterator[(Hash, E)] {
    var pos = 0
    def hasNext: Boolean = pos < mif.size
    def next(): (Hash, E) = {
      if (pos < mif.size) {
        mapped.position(pos * HashSpan)
        val rawHash = new Array[Byte](Hash.HashLength)
        mapped.get(rawHash)
        val eBuf = new Array[Byte](encoder.EBytes)
        mapped.get(eBuf)
        pos += 1
        (Hash.raw(rawHash), encoder.decode(ByteBuffer.wrap(eBuf)))
      } else
        throw new NoSuchElementException("next on empty iterator")
    }
  }
}
