ssh password: saferegionssafecloud

docker run --rm --name="cluster1" --net ncwork saferegions -s 0 6262 cluster2 6262 6272 6272
docker run --rm --name="cluster2" --net ncwork saferegions -s 1 6262 cluster3 6262 6272 6272
docker run --rm --name="cluster3" --net ncwork saferegions -s 2 6262 cluster1 6262 6272 6272