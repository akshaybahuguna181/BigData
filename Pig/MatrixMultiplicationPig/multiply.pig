A = LOAD '$M' USING PigStorage(',') AS ( i:long, j:long, v:double);
B = LOAD '$N' USING PigStorage(',') AS ( I:long, J:long, V:double);
J = JOIN A BY j, B BY I;
K = FOREACH J GENERATE i AS x,J AS y, v*V AS mul;
L = GROUP K BY (x,y);  
M = FOREACH L GENERATE FLATTEN($0),SUM(K.mul);
STORE M INTO '$O' USING PigStorage (',');