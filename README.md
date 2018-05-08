# presto-unimas-functions

多项式拟合三角函数，优化球面坐标距离计算
geo_distance
select geo_distance(39.904211, 117.407395, 39.904211, 116.407395);

jdbc push down函数
select execute_statement('jdbc:postgresql://xx.xx.xx.xx:5432/las','xxxx','xxxx','select count(1) from xxx where a = 1');

