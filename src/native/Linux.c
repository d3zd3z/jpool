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

/* Note that Scala doesn't generate static class, but singleton
 * objects. */

static jmethodID makePair;
static jmethodID readdirError;
static jmethodID lstatError;
static jmethodID infoZero;
static jmethodID infoPlus;

static jclass ListBufferClass;
static jmethodID lbCtor;
static jmethodID lbAppend;
static jmethodID lbToList;

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
}

JNIEXPORT jstring JNICALL Java_org_davidb_jpool_Linux_00024_message
	(JNIEnv *env, jobject obj)
{
	return (*env)->NewStringUTF(env, "Hello world");
}

JNIEXPORT jobject JNICALL Java_org_davidb_jpool_Linux_00024_readDir
	(JNIEnv *env, jobject obj, jstring path)
{
	/* Extract the name from the string.  Linux supports invalid
	 * UTF-8, so just use 8859-1 encoding, ignoring the high-byte.
	 * (for now). */
	const int len = (*env)->GetStringLength(env, path);
	char buf[len+1];
	int i;
	const jchar *src = (*env)->GetStringChars(env, path, NULL);
	for (i = 0; i < len; i++)
		buf[i] = src[i];
	buf[len] = 0;
	(*env)->ReleaseStringChars(env, path, src);

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
		jchar buf[len];
		int i;
		for (i = 0; i < len; i++)
			buf[i] = ent->d_name[i];

		jstring name = (*env)->NewString(env, buf, len);
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
	const int len = (*env)->GetStringLength(env, path);
	char buf[len+1];
	int i;
	const jchar *src = (*env)->GetStringChars(env, path, NULL);
	for (i = 0; i < len; i++)
		buf[i] = src[i];
	buf[len] = 0;
	(*env)->ReleaseStringChars(env, path, src);

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

jint JNI_OnLoad(JavaVM *vm, void *reserved)
{
	// printf("onLoad\n");
	return JNI_VERSION_1_2;
}

void JNI_OnUnload(JavaVM *vm, void *reserved)
{
	// printf("onUnload\n");
}
