#!/bin/sh
cmd="sftp_transfer"

source_host=$SOURCE_HOST
source_port=$SOURCE_PORT
source_user=$SOURCE_USER
source_pass=$SOURCE_PASSWORD
source_path=$SOURCE_PATH
dest_host=$DESTINATION_HOST
dest_port=$DESTINATION_PORT
dest_user=$DESTINATION_USER
dest_pass=$DESTINATION_PASSWORD
dest_path=$DESTINATION_PATH

if [ -z "$source_path" ]; then
    echo "source-path is required"
    exit 1
fi
if [ -z "$dest_path" ]; then
    echo "dest-path is required"
    exit 1
fi

if [ -n "$source_host" ]; then
    cmd="$cmd --source-host $source_host"
fi

if [ -n "$source_port" ]; then
    cmd="$cmd --source-port $source_port"
fi
if [ -n "$source_user" ]; then
    cmd="$cmd --source-user $source_user"
fi
if [ -n "$source_pass" ]; then
    cmd="$cmd --source-password $source_pass"
fi
cmd="$cmd --source-path $source_path"

if [ -n "$dest_host" ]; then
    cmd="$cmd --dest-host $dest_host"
fi
if [ -n "$dest_port" ]; then
    cmd="$cmd --dest-port $dest_port"
fi
if [ -n "$dest_user" ]; then
    cmd="$cmd --dest-user $dest_user"
fi
if [ -n "$dest_pass" ]; then
    cmd="$cmd --dest-password $dest_pass"
fi

cmd="$cmd --dest-path $dest_path"
echo $cmd
eval "$cmd"
