#!/bin/bash

# - My Variables
MYHOME="/home/juliux/Documents/repository/jsonSchemaValidator"
STAGE1_PATH="/home/juliux/Documents/repository/jsonSchemaValidator"
STAGE2_PATH="/home/juliux/Documents/repository/jsonSchemaValidator"
STAGE3_PATH="/home/juliux/Documents/repository/jsonSchemaValidator"
STAGE4_PATH="/home/juliux/Documents/repository/jsonSchemaValidator"
FINAL_STAGE_PATH="/home/juliux/Documents/repository/jsonSchemaValidator"
STAGE1_REGEX="sessionlog"
STAGE2_REGEX="auditlog"
STAGE3_REGEX="STAGE1"
STAGE4_REGEX="STAGE2"
STAGE1_ARE_COMPRESED="NO"
STAGE2_ARE_COMPRESED="NO"
STAGE1_COMPRESS_FORMAT="BZ2"
STAGE2_COMPRESS_FORMAT="BZ2"
FINAL_STAGE_FILE="FINAL_STAGE_DATABASE_FILE.sqlite"

# - Search variables
SEARCH_PATH="/home/juliux/Documents/repository/jsonSchemaValidator"
FINAL_DATABASE=$FINAL_STAGE_FILE

# - Commands
TRANSACTION_CHECKER="trans_checker_stats.py"
MYDUMMYFILE="FINAL_STAGE_DATABASE_FILE.sqlite"

# - Functions
function STAGE1 {
    
    #$1: STAGE1_PATH | $2: STAGE1_REGEX | $3:STAGE1_ARE_COMPRESED | $4:TRANSACTION_CHECKER | $5:STAGE1_COMPRESS_FORMAT
    
    STAGE1_PATH=$1
    STAGE1_REGEX=$2
    STAGE1_ARE_COMPRESED=$3
    TRANSACTION_CHECKER=$4
    STAGE1_COMPRESS_FORMAT=$5

    echo "=====START STAGE1====="   
    
    cd $STAGE1_PATH
    
    if [ $STAGE1_ARE_COMPRESED = 'YES' ] 
    then
	 case $STAGE1_COMPRESS_FORMAT in  
	
	 bz2)
		echo "BZ2"
		continue;;
	 gz)
		 echo "GZ"
		 continue;;
	 esac
     else
	     for file in $( ls -lrt $STAGE1_REGEX*.log | awk ' { print $9 } ' ) 
             do
		     echo $file
		     ./$TRANSACTION_CHECKER $file sessionlog
             done
     fi
     cd -
}

function STAGE2 {
    
    #$1: STAGE2_PATH | $2: STAGE2_REGEX | $3:STAGE2_ARE_COMPRESED | $4:TRANSACTION_CHECKER | $5:STAGE2_COMPRESS_FORMAT
    
    STAGE2_PATH=$1
    STAGE2_REGEX=$2
    STAGE2_ARE_COMPRESED=$3
    TRANSACTION_CHECKER=$4
    STAGE2_COMPRESS_FORMAT=$5

    echo "=====START STAGE2====="   
    
    cd $STAGE2_PATH
    
    if [ $STAGE2_ARE_COMPRESED = 'YES' ] 
    then
	 case $STAGE2_COMPRESS_FORMAT in  
	
	 bz2)
		echo "BZ2"
		continue;;
	 gz)
		 echo "GZ"
		 continue;;
	 esac
     else
	     for file in $( ls -lrt $STAGE2_REGEX*.log | awk ' { print $9 } ' ) 
             do
		     echo $file
		     ./$TRANSACTION_CHECKER $file auditlog
             done
     fi
     cd -
}

function STAGE3 {
    
    #$1: STAGE3_PATH | $2: STAGE3_REGEX
    
    STAGE3_PATH=$1
    STAGE3_REGEX=$2

    echo "=====START STAGE3====="   
    
    cd $STAGE3_PATH
    for file in $( ls -lrt *$STAGE3_REGEX* | awk ' { print $9 } ' ) 
    do
        echo $file
        ./$TRANSACTION_CHECKER $file stage3
    done
    cd -
}

function STAGE4 {
    
    #$1: STAGE4_PATH | $2: STAGE4_REGEX
    
    STAGE4_PATH=$1
    STAGE4_REGEX=$2

    echo "=====START STAGE4====="   
    
    cd $STAGE4_PATH
    for file in $( ls -lrt *$STAGE4_REGEX* | awk ' { print $9 } ' ) 
    do
        echo $file
        ./$TRANSACTION_CHECKER $file stage4
    done
    cd -
}

function FINAL_STAGE {
    
    #$1: FINAL_STAGE_PATH | $2: FINAL_STAGE_FILE
    
    FINAL_STAGE_PATH=$1
    FINAL_STAGE_FILE=$2

    echo "=====START FINAL_STAGE====="   
    
    cd $FINAL_STAGE_PATH
    touch $FINAL_STAGE_FILE
    ./$TRANSACTION_CHECKER $FINAL_STAGE_FILE finalstage
    cd -
}

function DATA_SEARCH {

    #$1: SEARCH_PATH | $2: STAGE3_REGEX | $3: STAGE4_REGEX | $4 FINAL_DATABASE
    
    SEARCH_PATH=$1
    STAGE3_REGEX=$2
    STAGE4_REGEX=$3
    FINAL_DATABASE=$4

    # --STAGE3_REGEX="STAGE1" --SESSION
    # --STAGE4_REGEX="STAGE2" --AUDIT

    
    echo "=====START DATA_SEARCH====="

    cd $SEARCH_PATH
    for AUDIT_FILE in $( ls -lrt *$STAGE4_REGEX* | awk ' { print $9 } ' )
    do
	     echo $AUDIT_FILE
	     # - auditlog-20190501074827.137204.log.yit61JTiO.STAGE2.sqlite
	     searchname=$( echo $AUDIT_FILE | awk -F- ' { print $2 } ' | awk -F. ' { print $1 } ' ) 
	     #echo $searchname
	     regexname=$( echo ${searchname:0:10} )
	     #echo $regexname
	     for SESSION_FILE in $( ls -lrt *$regexname*$STAGE3_REGEX* | awk ' { print $9 } ')
	     do
		     echo $SESSION_FILE
		     # - Compare the audit file with the session file
		     ./$TRANSACTION_CHECKER $AUDIT_FILE datasearch $SESSION_FILE $FINAL_DATABASE
             done
    done
}

function CONSOLIDATION {

    #$1: MYHOME $2: MYFILE
    MYHOME=$1
    MYFILE=$2
    
    echo "=====START CONSOLIDATION====="

    cd $MYHOME
    ./$TRANSACTION_CHECKER $MYFILE consolidation $MYHOME
}

function LOAD_SESSION_DATA {
    
    #$1: STAGE4_PATH | $2: STAGE4_REGEX
    
    STAGE4_PATH=$1
    STAGE4_REGEX=$2

    echo "=====START LOAD SESSION DATA====="   
    
    cd $STAGE4_PATH
    for file in $( ls -lrt *$STAGE4_REGEX* | awk ' { print $9 } ' ) 
    do
        echo $file
        ./$TRANSACTION_CHECKER $file load_session_data
    done
    cd -
}

function LOAD_AUDIT_DATA {
    
    #$1: STAGE4_PATH | $2: STAGE4_REGEX
    
    STAGE4_PATH=$1
    STAGE4_REGEX=$2

    echo "=====START LOAD AUDIT DATA====="   
    
    cd $STAGE4_PATH
    for file in $( ls -lrt *$STAGE4_REGEX* | awk ' { print $9 } ' ) 
    do
        echo $file
        ./$TRANSACTION_CHECKER $file load_audit_data
    done
    cd -
}

#STAGE1 $STAGE1_PATH $STAGE1_REGEX $STAGE1_ARE_COMPRESED $TRANSACTION_CHECKER $STAGE1_COMPRESS_FORMAT
#STAGE2 $STAGE2_PATH $STAGE2_REGEX $STAGE2_ARE_COMPRESED $TRANSACTION_CHECKER $STAGE2_COMPRESS_FORMAT
#STAGE3 $STAGE3_PATH $STAGE3_REGEX 
#STAGE4 $STAGE4_PATH $STAGE4_REGEX 
#FINAL_STAGE $FINAL_STAGE_PATH $FINAL_STAGE_FILE

# - Data consolidation

#DATA_SEARCH $SEARCH_PATH $STAGE3_REGEX $STAGE4_REGEX $FINAL_DATABASE
#CONSOLIDATION $MYHOME $MYDUMMYFILE
#LOAD_SESSION_DATA $MYHOME $STAGE3_REGEX
LOAD_AUDIT_DATA $MYHOME $STAGE4_REGEX
