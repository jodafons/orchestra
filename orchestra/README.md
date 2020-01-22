

# How to install Postgres and PgAdmin4 in your local server?


The postgres database was set to restart always when the system reboot on the main node machine `(146.164.147.170)` at LPS lab. The files and the creation script remains in an offuscated file in `home/rancher/.postgres` where we store all persistent files (volume) created by the postgres server. You must be sudo (for security reasons) to enter or operate the docker in this machine. 

This is the creation script used to setup the database server at LPS:

```bash
# in your root dir
docker run --name postgres --restart=always --network=postgres-network -e "POSTGRES_PASSWORD=postgres" -p 5432:5432 -v $PWD/volume:/var/lib/postgresql/data -d postgres
docker run --name pgadmin --restart=always --network=postgres-network -p 15432:80 -e "PGADMIN_DEFAULT_EMAIL=username@lps.ufrj.br" -e "PGADMIN_DEFAULT_PASSWORD=password" -d dpage/pgadmin4
```

To connect the pgadmin console at your local machine you must apply the follow command in your terminal:

```bash
ssh -L 15432:146.164.147.170:15432 username@bastion.lps.ufrj.br
```

This will open an port foward and externalize the server pgadmin port to your `localhost`. Now you will be able to use the pgadmin in your web browser.


## Connect the pgadmin to your postgres database:

- Click on Add Server;
- In the general form: (name=LPS);
- In the connection form: (hostname=146.164.147.170, port=5432, username=postgres, password=postgres)
- Click on connect. Now you will be able to administrate the database using the pgadmin web browser interface.


# How to launch and configure the Orchestra:

## Create the orchestra database:

To write the database objects, create the list of users and the list of machines you must run the script in `orchestra/orchestra/python/db/models/init_rancher.py`. If you have an database installed at the postgres server, you must remove all tables using the pgadmin first otherwise you will not be able to run this script.


## Configure the nodes that will be used bu the orchestra:

On your local pgadmin console connect with the LPS database. After you stablish one connection click in `Tools->QueryTool` on top of the dashboard. This will open the query editor. To access all config nodes run this command:

```bash
SELECT * FROM NODE
```

After run this command you will be able to edit the list of nodes machines registered in the database.
Each line is composed by the follow columns:
- machine name in the LPS cluster;
- CPUJobs: The number of slots allocated to run using the CPU hardware;
- GPUJobs: The number of slots allocated to run using the GPU hardware;
- maxCPUJobs: The number of slots allow to run using the CPU hardware (default is 30);
- maxGPUJobs: The number of slots allow to run using the GPU hardware (default is the number of GPUs installed);
- queueName: The name of the queue (default is lps, but can be expanded to sdumont, cern or loboc for future);
- others job counts control used in the dashboard painel.


To increase or decreased the number of slots for each much just double click at the CPU/GPUJobs field and change the value. After edit you just send the command (F6 or Save data changes) to the server and the orchestra will take care to update the schedule mechanism.


## Launch the orchestra:








# How to Install NVIDIA device plugin for Kubernetes?

## Table of Contents

- [About](#about)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
  - [Preparing your GPU Nodes](#preparing-your-gpu-nodes)
  - [Enabling GPU Support in Kubernetes](#enabling-gpu-support-in-kubernetes)
  - [Running GPU Jobs](#running-gpu-jobs)
- [Docs](#docs)
- [Changelog](#changelog)
- [Issues and Contributing](#issues-and-contributing)


## About

The NVIDIA device plugin for Kubernetes is a Daemonset that allows you to automatically:
- Expose the number of GPUs on each nodes of your cluster
- Keep track of the health of your GPUs
- Run GPU enabled containers in your Kubernetes cluster.

This repository contains NVIDIA's official implementation of the [Kubernetes device plugin](https://github.com/kubernetes/community/blob/master/contributors/design-proposals/resource-management/device-plugin.md).

## Prerequisites

The list of prerequisites for running the NVIDIA device plugin is described below:
* NVIDIA drivers ~= 361.93
* nvidia-docker version > 2.0 (see how to [install](https://github.com/NVIDIA/nvidia-docker) and it's [prerequisites](https://github.com/nvidia/nvidia-docker/wiki/Installation-\(version-2.0\)#prerequisites))
* docker configured with nvidia as the [default runtime](https://github.com/NVIDIA/nvidia-docker/wiki/Advanced-topics#default-runtime).
* Kubernetes version >= 1.10

## Quick Start

### Preparing your GPU Nodes

The following steps need to be executed on all your GPU nodes.
This README assumes that the NVIDIA drivers and nvidia-docker have been installed.

Note that you need to install the nvidia-docker2 package and not the nvidia-container-toolkit.
This is because the new `--gpus` options hasn't reached kubernetes yet. Example:
```bash
# Add the package repositories
$ distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
$ curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
$ curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | sudo tee /etc/apt/sources.list.d/nvidia-docker.list

$ sudo apt-get update && sudo apt-get install -y nvidia-docker2
$ sudo systemctl restart docker
```

You will need to enable the nvidia runtime as your default runtime on your node.
We will be editing the docker daemon config file which is usually present at `/etc/docker/daemon.json`:
```json
{
    "default-runtime": "nvidia",
    "runtimes": {
        "nvidia": {
            "path": "/usr/bin/nvidia-container-runtime",
            "runtimeArgs": []
        }
    }
}
```
> *if `runtimes` is not already present, head to the install page of [nvidia-docker](https://github.com/NVIDIA/nvidia-docker)*

### Enabling GPU Support in Kubernetes

Once you have enabled this option on *all* the GPU nodes you wish to use,
you can then enable GPU support in your cluster by deploying the following Daemonset:

```shell
$ kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/1.0.0-beta3/nvidia-device-plugin.yml
```

### Running GPU Jobs

NVIDIA GPUs can now be consumed via container level resource requirements using the resource name nvidia.com/gpu:
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gpu-pod
spec:
  containers:
    - name: cuda-container
      image: nvidia/cuda:9.0-devel
      resources:
        limits:
          nvidia.com/gpu: 2 # requesting 2 GPUs
    - name: digits-container
      image: nvidia/digits:6.0
      resources:
        limits:
          nvidia.com/gpu: 2 # requesting 2 GPUs
```

> **WARNING:** *if you don't request GPUs when using the device plugin with NVIDIA images all
> the GPUs on the machine will be exposed inside your container.*

