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

jint JNI_OnLoad(JavaVM *vm, void *reserved)
{
	// printf("onLoad\n");
	return JNI_VERSION_1_2;
}

void JNI_OnUnload(JavaVM *vm, void *reserved)
{
	// printf("onUnload\n");
}
