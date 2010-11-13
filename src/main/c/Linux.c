/* Linux native JNI. */

#define _FILE_OFFSET_BITS 64
#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <org_davidb_jpool_Linux__.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <alloca.h>

/* Allocate a stack variable called _cname that will be stack
 * allocated, and contain the _raw_ decoded characters from string
 * _jname.  _cname must be a simple identifier. */
#define JSTRING_TO_C_STACK(_env, _cname, _jname) \
	char *_cname; \
	{ \
		const int _len = (*(_env))->GetStringLength((_env), (_jname)); \
		_cname = alloca(_len); \
		int _i; \
		const jchar *_src = (*(_env))->GetStringChars((_env), (_jname), NULL); \
		for (_i = 0; _i < _len; _i++) { \
			_cname[_i] = _src[_i]; \
		} \
		_cname[_len] = 0; \
		(*(_env))->ReleaseStringChars((_env), (_jname), _src); \
	}

/* Allocate a Java string (local ref) from the given raw C string,
 * with the given length (not using the nul termination). */
static jstring c_to_jstring(JNIEnv *env, char *str, int len)
{
	jchar buf[len];
	int i;
	for (i = 0; i < len; i++)
		buf[i] = str[i] & 0xFF;
	return (*env)->NewString(env, buf, len);
}

/* Note that Scala doesn't generate static class, but singleton
 * objects. */

static jmethodID makePair;
static jmethodID throwNativeError;
static jmethodID infoZero;
static jmethodID infoPlus;

static jclass ListBufferClass;
static jmethodID lbCtor;
static jmethodID lbAppend;
static jmethodID lbToList;

static jclass oomClass;

static jmethodID function1Apply;

static jclass ByteBufferClass;
static jmethodID bbWrap;

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_setup
	(JNIEnv *env, jobject obj)
{
	jclass tmp;

	jclass clazz = (*env)->GetObjectClass(env, obj);
	makePair = (*env)->GetMethodID(env, clazz, "makePair",
			"(Ljava/lang/String;J)Lscala/Tuple2;");
	if (makePair == NULL)
		return;

	throwNativeError = (*env)->GetMethodID(env, clazz, "throwNativeError",
			"(Ljava/lang/String;Ljava/lang/String;I)Lscala/runtime/Nothing$;");
	if (throwNativeError == NULL)
		return;

	infoZero = (*env)->GetMethodID(env, clazz, "infoZero",
			"()Lscala/collection/immutable/Map;");
	if (infoZero == NULL)
		return;

	infoPlus = (*env)->GetMethodID(env, clazz, "infoPlus",
			"(Lscala/collection/immutable/Map;Ljava/lang/String;Ljava/lang/String;)"
			"Lscala/collection/immutable/Map;");
	if (infoPlus == NULL)
		return;

	tmp = (*env)->FindClass(env, "scala/collection/mutable/ListBuffer");
	if (tmp == NULL)
		return;
	ListBufferClass = (*env)->NewGlobalRef(env, tmp);
	if (ListBufferClass == NULL)
		return;

	lbCtor = (*env)->GetMethodID(env, ListBufferClass, "<init>", "()V");
	if (lbCtor == NULL)
		return;

	lbAppend = (*env)->GetMethodID(env, ListBufferClass, "$plus$eq",
			"(Ljava/lang/Object;)Lscala/collection/mutable/ListBuffer;");
	if (lbAppend == NULL)
		return;

	lbToList = (*env)->GetMethodID(env, ListBufferClass, "toList",
			"()Lscala/collection/immutable/List;");
	if (lbToList == NULL)
		return;

	tmp = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
	if (tmp == NULL)
		return;
	oomClass = (*env)->NewGlobalRef(env, tmp);
	if (oomClass == NULL)
		return;

	jclass function1 = (*env)->FindClass(env, "scala/Function1");
	if (function1 == NULL)
		return;

	function1Apply = (*env)->GetMethodID(env, function1, "apply",
			"(Ljava/lang/Object;)Ljava/lang/Object;");
	if (function1Apply == NULL)
		return;

	tmp = (*env)->FindClass(env, "java/nio/ByteBuffer");
	if (tmp == NULL)
		return;
	ByteBufferClass = (*env)->NewGlobalRef(env, tmp);
	if (ByteBufferClass == NULL)
		return;

	bbWrap = (*env)->GetStaticMethodID(env, ByteBufferClass, "wrap",
			"([BII)Ljava/nio/ByteBuffer;");
	if (bbWrap == NULL)
		return;
}

/* Error for a call with a path. */
static void path_error(JNIEnv *env, jobject obj, jstring path, char *callName)
{
	jstring myName = (*env)->NewStringUTF(env, callName);
	if (myName != NULL)
		(*env)->CallObjectMethod(env, obj, throwNativeError,
				myName, path, (jint) errno);
}

JNIEXPORT jstring JNICALL Java_org_davidb_jpool_Linux_00024_message
	(JNIEnv *env, jobject obj)
{
	return (*env)->NewStringUTF(env, "Hello world");
}

JNIEXPORT jobject JNICALL Java_org_davidb_jpool_Linux_00024_readDir
	(JNIEnv *env, jobject obj, jstring path)
{
	JSTRING_TO_C_STACK(env, buf, path);

	DIR *dirp = opendir(buf);

	if (dirp == NULL) {
		path_error(env, obj, path, "readDir");
		return NULL;
	}

	/* Use the Scala ListBuffer. */
	jobject lbuf = (*env)->NewObject(env, ListBufferClass, lbCtor);
	if (lbuf == NULL)
		goto failure;

	while (1) {
		struct dirent *ent = readdir(dirp);
		if (ent == NULL)  /* TODO: Distinguish EOF from error. */
			break;

		int len = strlen(ent->d_name);
		if (len == 1 && ent->d_name[0] == '.')
			continue;
		if (len == 2 && ent->d_name[0] == '.' && ent->d_name[1] == '.')
			continue;
		jstring name = c_to_jstring(env, ent->d_name, len);
		if (name == NULL)
			goto failure;
		jobject pair = (*env)->CallObjectMethod(env, obj, makePair,
				(jobject)name, (jlong)ent->d_ino);
		(*env)->CallObjectMethod(env, lbuf, lbAppend, pair);
	}

	closedir(dirp);
	return (*env)->CallObjectMethod(env, lbuf, lbToList);

failure:
	closedir(dirp);
	return NULL;
}

/* Set a property. */
static jobject setProp(JNIEnv *env, jobject obj, jobject map, char *key, char *val)
{
	jobject ks, vs;
	ks = (*env)->NewStringUTF(env, key);
	if (ks == NULL)
		return NULL;
	vs = (*env)->NewStringUTF(env, val);
	if (vs == NULL)
		return NULL;
	jobject nmap = (*env)->CallObjectMethod(env, obj, infoPlus, map, ks, vs);
	(*env)->DeleteLocalRef(env, ks);
	(*env)->DeleteLocalRef(env, vs);
	(*env)->DeleteLocalRef(env, map);
	return nmap;
}

static jobject lstat_convert(JNIEnv *env, jobject obj, jstring path, struct stat *sbuf)
{
	jobject map = (*env)->CallObjectMethod(env, obj, infoZero);

	/* Determine the kind. */
	char *v;
	if (S_ISREG(sbuf->st_mode))
		v = "REG";
	else if (S_ISDIR(sbuf->st_mode))
		v = "DIR";
	else if (S_ISCHR(sbuf->st_mode))
		v = "CHR";
	else if (S_ISBLK(sbuf->st_mode))
		v = "BLK";
	else if (S_ISFIFO(sbuf->st_mode))
		v = "FIFO";
	else if (S_ISLNK(sbuf->st_mode))
		v = "LNK";
	else if (S_ISSOCK(sbuf->st_mode))
		v = "SOCK";
	else if (S_ISSOCK(sbuf->st_mode))
		v = "DIR";
	else {
		path_error(env, obj, path, "lstat");
		return NULL;
	}
	map = setProp(env, obj, map, "*kind*", v);
	if (map == NULL)
		return;

	char tmp[40];

	sprintf(tmp, "%lld", (long long)(sbuf->st_mode & (~S_IFMT)));
	map = setProp(env, obj, map, "mode", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf->st_dev);
	map = setProp(env, obj, map, "dev", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf->st_ino);
	map = setProp(env, obj, map, "ino", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf->st_nlink);
	map = setProp(env, obj, map, "nlink", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf->st_uid);
	map = setProp(env, obj, map, "uid", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf->st_gid);
	map = setProp(env, obj, map, "gid", tmp);
	if (map == NULL)
		return;

	if (S_ISCHR(sbuf->st_mode) || S_ISBLK(sbuf->st_mode)) {
		sprintf(tmp, "%lld", (long long)sbuf->st_rdev);
		map = setProp(env, obj, map, "rdev", tmp);
		if (map == NULL)
			return;
	}

	sprintf(tmp, "%lld", (long long)sbuf->st_size);
	map = setProp(env, obj, map, "size", tmp);
	if (map == NULL)
		return;

	// TODO: Store fractional time.
	sprintf(tmp, "%lld", (long long)sbuf->st_mtime);
	map = setProp(env, obj, map, "mtime", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf->st_ctime);
	map = setProp(env, obj, map, "ctime", tmp);
	if (map == NULL)
		return;

	return map;
}

JNIEXPORT jobject JNICALL Java_org_davidb_jpool_Linux_00024_stat
	(JNIEnv *env, jobject obj, jstring path)
{
	JSTRING_TO_C_STACK(env, buf, path);

	struct stat sbuf;
	int res = stat(buf, &sbuf);

	if (res < 0) {
		path_error(env, obj, path, "stat");
		return NULL;
	}

	return lstat_convert(env, obj, path, &sbuf);
}

JNIEXPORT jobject JNICALL Java_org_davidb_jpool_Linux_00024_lstat
	(JNIEnv *env, jobject obj, jstring path)
{
	JSTRING_TO_C_STACK(env, buf, path);

	struct stat sbuf;
	int res = lstat(buf, &sbuf);

	if (res < 0) {
		path_error(env, obj, path, "lstat");
		return NULL;
	}

	return lstat_convert(env, obj, path, &sbuf);
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_symlink
	(JNIEnv *env, jobject obj, jstring oldPath, jstring newPath)
{
	JSTRING_TO_C_STACK(env, cOldPath, oldPath);
	JSTRING_TO_C_STACK(env, cNewPath, newPath);

	int result = symlink(cOldPath, cNewPath);
	if (result != 0) {
		path_error(env, obj, newPath, "symlink");
		return;
	}
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_link
	(JNIEnv *env, jobject obj, jstring oldPath, jstring newPath)
{
	JSTRING_TO_C_STACK(env, cOldPath, oldPath);
	JSTRING_TO_C_STACK(env, cNewPath, newPath);

	int result = link(cOldPath, cNewPath);
	if (result != 0) {
		path_error(env, obj, newPath, "link");
		return;
	}
}

JNIEXPORT jstring JNICALL Java_org_davidb_jpool_Linux_00024_readlink
	(JNIEnv *env, jobject obj, jstring path)
{
	JSTRING_TO_C_STACK(env, cpath, path);

	int size = 128;
	char *buffer = malloc(size);
	if (buffer == NULL) {
		(*env)->ThrowNew(env, oomClass, "Unable to allocate readlink buffer");
		return NULL;
	}

	while (1) {
		int count = readlink(cpath, buffer, size);
		if (count < 0) {
			free(buffer);
			path_error(env, obj, path, "readlink");
			return NULL;
		}
		else if (count == size) {
			free(buffer);
			size *= 2;
			buffer = malloc(size);
			if (buffer == NULL) {
				(*env)->ThrowNew(env, oomClass, "Unable to allocate readlink buffer");
				return NULL;
			}
		} else {
			jstring result = c_to_jstring(env, buffer, count);
			free(buffer);
			return result;
		}
	}
}

JNIEXPORT jint JNICALL Java_org_davidb_jpool_Linux_00024_openForWrite
	(JNIEnv *env, jobject obj, jstring path)
{
	JSTRING_TO_C_STACK(env, cpath, path);
	int fd = open(cpath, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0) {
		path_error(env, obj, path, "open");
		return -1;
	}

	return fd;
}

JNIEXPORT jint JNICALL Java_org_davidb_jpool_Linux_00024_openForRead
	(JNIEnv *env, jobject obj, jstring path)
{
	JSTRING_TO_C_STACK(env, cpath, path);
	int fd = open(cpath, O_RDONLY | O_NOATIME);
	/* Non-root users aren't permitted to open without changing
	 * atime on files they don't own.  If this happens, try
	 * opening the normal way. */
	if (fd < 0 && errno == EPERM)
		fd = open(cpath, O_RDONLY);
	if (fd < 0) {
		path_error(env, obj, path, "open");
		return -1;
	}

	return fd;
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_close
	(JNIEnv *env, jobject obj, jint fd)
{
	int result = close(fd);
	if (result < 0) {
		path_error(env, obj, NULL, "close");
		return;
	}
}

/* Note that this assumes that the offset and length are within
 * bounds, since they are checked in the caller. */
JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_writeChunk
	(JNIEnv *env, jobject obj, jint fd, jbyteArray jbuffer, jint offset, jint length)
{
	jbyte *buffer = (*env)->GetByteArrayElements(env, jbuffer, NULL);
	if (buffer == NULL)
		return;

	while (length > 0) {
		int count = write(fd, buffer + offset, length);
		if (count <= 0) {
			path_error(env, obj, NULL, "write");
			goto cleanup;
		}
		length -= count;
		offset += count;
	}

cleanup:
	(*env)->ReleaseByteArrayElements(env, jbuffer, buffer, JNI_ABORT);
}

/* This also assumes that the offset and length are within bounds.
 * Returns number of bytes actually read, possibly zero. */
JNIEXPORT int JNICALL Java_org_davidb_jpool_Linux_00024_readChunk
	(JNIEnv *env, jobject obj, jint fd, jbyteArray jbuffer, jint offset, jint length)
{
	jbyte *buffer = (*env)->GetByteArrayElements(env, jbuffer, NULL);
	if (buffer == NULL)
		return;

	int total = 0;

	while (length > 0) {
		int count = read(fd, buffer + offset, length);
		if (count == 0)
			break;
		if (count < 0) {
			path_error(env, obj, NULL, "read");
			(*env)->ReleaseByteArrayElements(env, jbuffer, buffer, JNI_ABORT);
			return;
		}
		length -= count;
		offset += count;
		total += count;
	}

	(*env)->ReleaseByteArrayElements(env, jbuffer, buffer, 0);

	return total;
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_mkdir
	(JNIEnv *env, jobject obj, jstring path)
{
	JSTRING_TO_C_STACK(env, cpath, path);

	int result = mkdir(cpath, 0700);
	if (result != 0) {
		path_error(env, obj, path, "mkdir");
		return;
	}
}

JNIEXPORT jint JNICALL Java_org_davidb_jpool_Linux_00024_geteuid
	(JNIEnv *env, jobject obj)
{
	return geteuid();
}

JNIEXPORT jint JNICALL Java_org_davidb_jpool_Linux_00024_umask
	(JNIEnv *env, jobject obj, jint mask)
{
	return umask(mask);
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_utime
	(JNIEnv *env, jobject obj, jstring path, jlong atime, jlong mtime)
{
	JSTRING_TO_C_STACK(env, cpath, path);
	struct timeval times[2];
	times[0].tv_sec = atime;
	times[0].tv_usec = 0;
	times[1].tv_sec = mtime;
	times[1].tv_usec = 0;

	int result = utimes(cpath, times);
	if (result != 0) {
		path_error(env, obj, path, "mkdir");
		return;
	}
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_chown
	(JNIEnv *env, jobject obj, jstring path, jlong uid, jlong gid)
{
	JSTRING_TO_C_STACK(env, cpath, path);
	if (chown(cpath, uid, gid) != 0)
		path_error(env, obj, path, "chown");
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_lchown
	(JNIEnv *env, jobject obj, jstring path, jlong uid, jlong gid)
{
	JSTRING_TO_C_STACK(env, cpath, path);
	if (lchown(cpath, uid, gid) != 0)
		path_error(env, obj, path, "lchown");
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_chmod
	(JNIEnv *env, jobject obj, jstring path, jlong mode)
{
	JSTRING_TO_C_STACK(env, cpath, path);
	if (chmod(cpath, mode) != 0)
		path_error(env, obj, path, "chmod");
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_makeSpecial
	(JNIEnv *env, jobject obj, jstring path, jstring kind, jlong mode, jlong dev)
{
	JSTRING_TO_C_STACK(env, cpath, path);

	mode_t nmode = mode;
	int failure = 0;
	const char *ckind = (*env)->GetStringUTFChars(env, kind, NULL);
	if (ckind == NULL) {
		path_error(env, obj, path, "mknod kind null");
		return;
	}
	if (strcmp(ckind, "BLK") == 0)
		nmode |= S_IFBLK;
	else if (strcmp(ckind, "CHR") == 0)
		nmode |= S_IFCHR;
	else if (strcmp(ckind, "FIFO") == 0)
		nmode |= S_IFIFO;
	else if (strcmp(ckind, "SOCK") == 0)
		nmode |= S_IFSOCK;
	else
		failure = 1;
	(*env)->ReleaseStringUTFChars(env, kind, ckind);

	if (failure) {
		path_error(env, obj, path, "mknod kind");
		return;
	}

	if (mknod(cpath, nmode, dev) != 0)
		path_error(env, obj, path, "mknod");
}

JNIEXPORT jstring JNICALL Java_org_davidb_jpool_Linux_00024_ttyname
	(JNIEnv *env, jobject obj, int fd)
{
	/* The ttyname_r call appears to be broken on linux (off by
	 * one).  If the name doesn't quite fit in the buffer, it will
	 * consider it success, but obliterate the last character with
	 * the nul.  To help, just make this big enough. */
	int size = 1024;
	char *buffer = malloc(size);
	if (buffer == NULL) {
		(*env)->ThrowNew(env, oomClass, "Unable to allocate ttyname buffer");
		return NULL;
	}

	int err = ttyname_r(fd, buffer, size);
	if (err != 0) {
		free(buffer);
		path_error(env, obj, NULL, "ttyname");
		return NULL;
	}

	jstring result = (*env)->NewStringUTF(env, buffer);
	free(buffer);
	return result;
}

/* Note that this is glibc specific, and depends on the _GNU_SOURCE
 * defined before including anything. */
JNIEXPORT jstring JNICALL Java_org_davidb_jpool_Linux_00024_strerror
	(JNIEnv *env, jobject obj, int errnum)
{
	char buf[128];
	char *msg = strerror_r(errnum, buf, sizeof(buf));
	return c_to_jstring(env, msg, strlen(msg));
}

jint JNI_OnLoad(JavaVM *vm, void *reserved)
{
	// printf("onLoad\n");
	return JNI_VERSION_1_2;
}

void JNI_OnUnload(JavaVM *vm, void *reserved)
{
	// printf("onUnload\n");
}
