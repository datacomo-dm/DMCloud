rm riakksf_main.o riakksf_main -f
gcc -std=c++0x -DBUILD_RIAKKSF_TOOL -g riakksf_main.cpp  riakksf.cc -o riakksf_main -lstdc++ -lrt
chmod +x riakksf_main

gcc -std=c++0x -DBUILD_RIAKKSF_TOOL -DREUSE_CLUSTER_FM_KEYSPACE_IN_FILE_MODE -g riakksf_main.cpp  riakksf.cc -o riakksf_main_reuse_cluster_fm_ks -lstdc++ -lrt
chmod +x riakksf_main_reuse_cluster_fm_ks

rm RiakKeyListGenerate.o RiakKeyListGenerate -f 
gcc -std=c++0x -g RiakKeyListGenerate.cpp -o riakkey_gen -lstdc++ -lrt
chmod +x riakkey_gen


