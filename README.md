# CubeFS

[![CNCF Status](https://img.shields.io/badge/cncf%20status-incubating-blue.svg)](https://www.cncf.io/projects)
[![Build Status](https://github.com/cubefs/cubefs/actions/workflows/ci.yml/badge.svg)](https://github.com/cubefs/cubefs/actions/workflows/ci.yml)
[![LICENSE](https://img.shields.io/github/license/cubefs/cubefs.svg)](https://github.com/cubefs/cubefs/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/cubefs/cubefs)](https://goreportcard.com/report/github.com/cubefs/cubefs)
[![Docs](https://readthedocs.org/projects/cubefs/badge/?version=latest)](https://cubefs.readthedocs.io/en/latest/?badge=latest)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/2761/badge)](https://bestpractices.coreinfrastructure.org/projects/2761)

|<img src="https://user-images.githubusercontent.com/5708406/91202310-31eaab80-e734-11ea-84fc-c1b1882ae71c.png" height="24"/>&nbsp;Community Meeting|
|------------------|
| The CubeFS Project holds bi-weekly community online meeting. To join or watch previous meeting notes and recordings, please see [meeting schedule](https://github.com/cubefs/community/wiki/Meeting-Schedule) and [meeting minutes](https://github.com/cubefs/community/wiki/Meeting-Agenda-and-Notes). |

**Note**: The `master` branch may be in an *unstable or even broken state* during development.
Please use [releases](https://github.com/cubefs/cubefs/releases) instead of the `master` branch in order to get a stable set of binaries.

<div width="100%" style="text-align:center;"><img alt="CubeFS" src="https://user-images.githubusercontent.com/12113219/150923746-d09409fc-78ca-42cb-a467-b356c3ef9f61.png" height="200"/></div>

## Contents

- [Overview](#overview)
- [Documents](#documents)
- [Benchmark](#benchmark)
- [Build CubeFS](#build-cubefs)
- [Yum Tools to Run a CubeFS Cluster for CentOS 7+](#yum-tools-to-run-a-cubefs-cluster-for-centos-7)
- [Run a CubeFS Cluster within Docker](#run-a-cubefs-cluster-within-docker)
- [Helm chart to Run a CubeFS Cluster in Kubernetes](#helm-chart-to-run-a-cubefs-cluster-in-kubernetes)
- [Reference](#reference)
- [Contributing](#contributing)
- [Reporting a security vulnerability](#reporting-a-security-vulnerability)
- [Community](#community)
- [Partners and Users](#partners-and-users)
- [License](#license)

## Overview

CubeFS (储宝文件系统 in Chinese) is a cloud-native storage platform that provides both POSIX-compliant and S3-compatible interfaces. It is hosted by the [Cloud Native Computing Foundation](https://cncf.io) (CNCF) as a [incubating](https://www.cncf.io/projects/) project.

CubeFS has been commonly used as the underlying storage infrastructure for online applications, database or data processing services and machine learning jobs orchestrated by Kubernetes. 
An advantage of doing so is to separate storage from compute - one can scale up or down based on the workload and independent of the other, providing total flexibility in matching resources to the actual storage and compute capacity required at any given time.

Some key features of CubeFS include:

- Multiple Access Protocol Support  
  Converge posix filesystem interfaces、S3-compatible interfaces and hdfs interfaces
- Metadata Highly Scalable  
  Elasticity, scalability and strong consistency of metadata
- High Performance  
  Specific performance optimizations for large/small files and sequential/random writes
- Multi-tenancy  
  Share the whole storage and resource utilization ratio
- Hybrid Cloud Acceleration  
  Accelerate Hybrid Cloud IO performance through multi-level caching
- Online Erasure Coding Subsystem  
  Provide high durability, high availability, low cost erasure coding storage system

We are committed to making CubeFS better and more mature. Please stay tuned. 

## Documents

English version: https://cubefs.readthedocs.io/en/latest/

Chinese version: https://cubefs.readthedocs.io/zh_CN/latest/

## Benchmark

Small file operation performance and scalability benchmark test by [mdtest](https://github.com/LLNL/mdtest).

<img src="https://raw.githubusercontent.com/cubefs/cubefs/master/docs/source/pic/cfs-small-file-benchmark.png" width="600" align=center/>

|File Size (KB)	|  1	|  2	|  4	|  8	|   16  |   32  |   64  |  128 |
|:-|:-|:-|:-|:-|:-|:-|:-|:-|
|Creation (TPS)	|70383	|70383	|73738	|74617	|69479	|67435	|47540	|27147 |
|Read (TPS)	    |108600	|118193	|118346	|122975	|116374	|110795	|90462	|62082 |
|Removal (TPS)	|87648	|84651	|83532	|79279	|85498	|86523	|80946	|84441 |
|Stat (TPS)	    |231961	|263270	|264207	|252309	|240244	|244906	|273576	|242930|

Refer to [cubefs.readthedocs.io](https://cubefs.readthedocs.io/en/latest/evaluation.html) for performance and scalability of `IO` and `Metadata`.

## Build CubeFS 
[how to set up and query a small CubeFS cluster](https://github.com/cubefs/cubefs/blob/master/INSTALL.md) 

## Kubernetes 
[Helm chart to Run a CubeFS Cluster in Kubernetes](https://github.com/cubefs/cubefs/blob/master/INSTALL.md)

## Reference

Haifeng Liu, et al., CFS: A Distributed File System for Large Scale Container Platforms. SIGMOD‘19, June 30-July 5, 2019, Amsterdam, Netherlands. 

For more information, please refer to https://dl.acm.org/citation.cfm?doid=3299869.3314046 and https://arxiv.org/abs/1911.03001

## Contributing

Recommend the standard GitHub flow based on forking and pull requests.<br>
See [CONTRIBUTING.md](CONTRIBUTING.md#workflow) for detail.

## Reporting a security vulnerability

See [security disclosure process](security/README.md) for detail.

## Community

- Mailing list: cubefs-users@groups.io
- Slack: [cubefs.slack.com](https://cubefs.slack.com/)
- WeChat: detail see [here](https://github.com/cubefs/cubefs/issues/604).

## Partners and Users

For a list of users and success stories see [ADOPTERS.md](ADOPTERS.md).

## License

CubeFS is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
For detail see [LICENSE](LICENSE) and [NOTICE](NOTICE).

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcubefs%2Fcfs.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcubefs%2Fcfs?ref=badge_large)
