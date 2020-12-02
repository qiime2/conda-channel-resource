# conda-channel-resource

[Concourse](http://concourse.ci/) [resource type](http://concourse.ci/resource-types.html)
for working with [conda](https://conda.io/docs/) channels.

## testing env

```bash
conda create -n conda-channel-resource-testing python=3.8
conda activate conda-channel-resource-testing
pip install docker-compose
cd testing
docker-compose -f cluster.yml up --build
```
