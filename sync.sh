#!/bin/bash

######################### Local Workspace Env ######################
WORKSPACE=/tmp/workspace
DATE=`date +%Y-%m-%d`
FILELIST=/logs/${DATE}
CONF_PATH=$WORKSPACE/sync.conf
SYNC_FILE_DIR=$WORKSPACE/sync_files
FAILED_HISTORY=$WORKSPACE/failed_history
THREADS=20
######################### Local Workspace Env ######################

######################## BUCKET CONFIG BEGION #######################

SRC_BUCKET=srcbucket
SRC_APPID=123456789
SRC_REGION=cd # cd/sh...
SRC_AK=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
SRC_SK=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb

DEST_BUCKET=destbucket
DEST_AK=cccccccccccccccccccccccccccccc
DEST_SK=dddddddddddddddddddddddddddddd
DEST_ENDPOINT=127.0.0.1

######################## BUCKET CONFIG END ##########################


function GenerateConf() {
    if [[ -e "${CONF_PATH}" ]]; then
        rm ${CONF_PATH}
    fi

    touch ${CONF_PATH}
    if [[ $? -ne 0 ]]; then
        echo "Generate Conf error."
        exit 1
    fi
    echo "[common]" >> ${CONF_PATH}
    echo "workspace=${WORKSPACE}" >> ${CONF_PATH}
    echo "threads=${THREADS}" >> ${CONF_PATH}
    echo "" >> ${CONF_PATH}

    echo "[source]" >> ${CONF_PATH}
    echo "type=cosv4" >> ${CONF_PATH}
    echo "accesskeyid=${SRC_AK}" >> ${CONF_PATH}
    echo "accesskeysecret=${SRC_SK}" >> ${CONF_PATH}
    echo "bucket=${SRC_BUCKET}" >> ${CONF_PATH}
    echo "appid=${SRC_APPID}" >> ${CONF_PATH}
    echo "region=${SRC_REGION}" >> ${CONF_PATH}
    echo "sync_files=${SYNC_FILE_DIR}" >> ${CONF_PATH}
    echo "" >> ${CONF_PATH}

    echo "[destination]" >> ${CONF_PATH}
    echo "type=s3" >> ${CONF_PATH}
    echo "accesskeyid=${DEST_AK}" >> ${CONF_PATH}
    echo "accesskeysecret=${DEST_SK}" >> ${CONF_PATH}
    echo "bucket=${DEST_BUCKET}" >> ${CONF_PATH}
    echo "endpoint=${DEST_ENDPOINT}" >> ${CONF_PATH}
    echo "prefix=/" >> ${CONF_PATH}
}

function Init() {
    if [[ ! -e ${WORKSPACE} ]]; then
        mkdir -p ${WORKSPACE}
        if [[ 0 -ne $? ]]; then
        	echo "Create workspace dir failed. workspace=${WORKSPACE}".
            exit 1
        fi
    fi
    
    if [[ x"$1" != x"all" ]]; then
        if [[ ! -e ${SYNC_FILE_DIR} ]]; then
            mkdir -p ${SYNC_FILE_DIR}
            if [[ 0 -ne $? ]]; then
        	    echo "Create sync file dir failed. sync_file_dir=${SYNC_FILE_DIR}".
                exit 1
            fi
        fi
    fi
    
    if [[ ! -e ${FAILED_HISTORY} ]]; then
        mkdir -p ${FAILED_HISTORY}
        if [[ 0 -ne $? ]]; then
        	echo "Create failed history dir failed. failed_histroy=${FAILED_HISTORY}".
            exit 1
        fi
    fi
}

Init $1
GenerateConf
if [[ x"$1" == x"all" ]]; then
    cos_migrate_tool -c ${CONF_PATH} 
else
    cos_migrate_tool -c ${CONF_PATH} --filelist=${FILELIST}
fi

if [[ $? -ne 0 ]]; then
    echo "Sync files error.".
    exit 2
fi

# Save failed_files
mv ${WORKSPACE}/failed_files.txt ${FAILED_HISTORY}/failed_files.${DATE}

if [[ x"$1" != x"all" ]]; then
    if [[ ! -e "${SYNC_FILE_DIR}/${DATE}" ]]; then
        echo "Download sync file error".
        exit 3
    fi
fi

if [[ -s ${FAILED_HISTORY}/failed_files.${DATE} ]]; then
    echo "Sync files error, check failed_files[${FAILED_HISTORY}/failed_files.${DATE}]" 
    exit 4
fi

exit 0
