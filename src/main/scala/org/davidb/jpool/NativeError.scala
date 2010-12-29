/**********************************************************************/
// Native errors.

package org.davidb.jpool

import java.io.IOException

class NativeError(val name: String, val path: String, val errno: Int, message: String)
  extends IOException(message)
