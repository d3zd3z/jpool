/* Linux native JNI. */

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <string.h>
#include <org_davidb_jpool_Linux_.h>

#include <sys/types.h>
#include <sys/stat.h>
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
static jmethodID readdirError;
static jmethodID lstatError;
static jmethodID readlinkError;
static jmethodID symlinkError;
static jmethodID infoZero;
static jmethodID infoPlus;

static jclass ListBufferClass;
static jmethodID lbCtor;
static jmethodID lbAppend;
static jmethodID lbToList;

static jclass oomClass;

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_setup
	(JNIEnv *env, jobject obj)
{
	jclass tmp;

	jclass clazz = (*env)->GetObjectClass(env, obj);
	makePair = (*env)->GetMethodID(env, clazz, "makePair",
			"(Ljava/lang/String;J)Lscala/Tuple2;");
	if (makePair == NULL)
		return;

	readdirError = (*env)->GetMethodID(env, clazz, "readdirError",
			"(Ljava/lang/String;I)Lscala/runtime/Nothing$;");
	if (readdirError == NULL)
		return;

	lstatError = (*env)->GetMethodID(env, clazz, "lstatError",
			"(Ljava/lang/String;I)Lscala/runtime/Nothing$;");
	if (lstatError == NULL)
		return;

	readlinkError = (*env)->GetMethodID(env, clazz, "readlinkError",
			"(Ljava/lang/String;I)Lscala/runtime/Nothing$;");
	if (readlinkError == NULL)
		return;

	symlinkError = (*env)->GetMethodID(env, clazz, "symlinkError",
			"(Ljava/lang/String;Ljava/lang/String;I)Lscala/runtime/Nothing$;");
	if (symlinkError == NULL)
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
			"(Ljava/lang/Object;)V");
	if (lbAppend == NULL)
		return;

	lbToList = (*env)->GetMethodID(env, ListBufferClass, "toList",
			"()Lscala/List;");
	if (lbToList == NULL)
		return;

	tmp = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
	if (tmp == NULL)
		return;
	oomClass = (*env)->NewGlobalRef(env, tmp);
	if (oomClass == NULL)
		return;
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
		(*env)->CallObjectMethod(env, obj, readdirError, path, (jint) errno);
		// This returns Nothing, so always raises an
		// exception.
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

JNIEXPORT jobject JNICALL Java_org_davidb_jpool_Linux_00024_lstat
	(JNIEnv *env, jobject obj, jstring path)
{
	JSTRING_TO_C_STACK(env, buf, path);

	struct stat sbuf;
	int res = lstat(buf, &sbuf);

	if (res < 0) {
		(*env)->CallObjectMethod(env, obj, lstatError, path, (jint) errno);
		return NULL;
	}

	jobject map = (*env)->CallObjectMethod(env, obj, infoZero);

	/* Determine the kind. */
	char *v;
	if (S_ISREG(sbuf.st_mode))
		v = "REG";
	else if (S_ISDIR(sbuf.st_mode))
		v = "DIR";
	else if (S_ISCHR(sbuf.st_mode))
		v = "CHR";
	else if (S_ISBLK(sbuf.st_mode))
		v = "BLK";
	else if (S_ISFIFO(sbuf.st_mode))
		v = "FIFO";
	else if (S_ISLNK(sbuf.st_mode))
		v = "LNK";
	else if (S_ISSOCK(sbuf.st_mode))
		v = "SOCK";
	else if (S_ISSOCK(sbuf.st_mode))
		v = "DIR";
	else {
		(*env)->CallObjectMethod(env, obj, lstatError, path, EINVAL);
		return NULL;
	}
	map = setProp(env, obj, map, "*kind*", v);
	if (map == NULL)
		return;

	char tmp[40];

	sprintf(tmp, "%lld", (long long)(sbuf.st_mode & (~S_IFMT)));
	map = setProp(env, obj, map, "mode", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf.st_dev);
	map = setProp(env, obj, map, "dev", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf.st_ino);
	map = setProp(env, obj, map, "ino", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf.st_nlink);
	map = setProp(env, obj, map, "nlink", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf.st_uid);
	map = setProp(env, obj, map, "uid", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf.st_gid);
	map = setProp(env, obj, map, "gid", tmp);
	if (map == NULL)
		return;

	if (S_ISCHR(sbuf.st_mode) || S_ISBLK(sbuf.st_mode)) {
		sprintf(tmp, "%lld", (long long)sbuf.st_rdev);
		map = setProp(env, obj, map, "rdev", tmp);
		if (map == NULL)
			return;
	}

	sprintf(tmp, "%lld", (long long)sbuf.st_size);
	map = setProp(env, obj, map, "size", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf.st_mtime);
	map = setProp(env, obj, map, "mtime", tmp);
	if (map == NULL)
		return;

	sprintf(tmp, "%lld", (long long)sbuf.st_ctime);
	map = setProp(env, obj, map, "ctime", tmp);
	if (map == NULL)
		return;

	return map;
}

JNIEXPORT void JNICALL Java_org_davidb_jpool_Linux_00024_symlink
	(JNIEnv *env, jobject obj, jstring oldPath, jstring newPath)
{
	JSTRING_TO_C_STACK(env, cOldPath, oldPath);
	JSTRING_TO_C_STACK(env, cNewPath, newPath);

	int result = symlink(cOldPath, cNewPath);
	if (result != 0) {
		(*env)->CallObjectMethod(env, obj, symlinkError, oldPath, newPath, (jint) errno);
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
			(*env)->CallObjectMethod(env, obj, readlinkError, path, (jint) errno);
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

jint JNI_OnLoad(JavaVM *vm, void *reserved)
{
	// printf("onLoad\n");
	return JNI_VERSION_1_2;
}

void JNI_OnUnload(JavaVM *vm, void *reserved)
{
	// printf("onUnload\n");
}
