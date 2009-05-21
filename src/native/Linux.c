/* Linux native JNI. */

#include <stdio.h>
#include <org_davidb_jpool_Linux_.h>

JNIEXPORT jstring JNICALL Java_org_davidb_jpool_Linux_00024_message
	(JNIEnv *env, jobject this)
{
	return (*env)->NewStringUTF(env, "Hello world");
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
