# ��#�ſ�ʼ��ʾ��һ����ע��
#[FTP]
#host= 192.192.192.2
#Username=dm4321
#Password=Admin@#@!
##�����Binaryģʽ�����ļ���Textmode����Ϊ0
#Textmode=0
#path=/

#����·����FTP/LOCALֻ����һ��������������ͬʱ����
[PARSER]
PARSERMODULE=/app/dm6/plugin/libcommontxt_parser.so
#ָ���ļ��ֽ�ģ����һ��ʱ����ڲ���������λ��ʱ��0-23����Start����С�ڻ����END��START=END=0����ʾ�ּ�ģ��ȫ�칤�����ּ�ģ����е��������ǣ�һ�Ǳ������������������ռϵͳ��Դ�������ڲ�ѯ�߷��ڱ�����Ӧ�ٶȱ�����
PARSERIDLESTART=4
PARSERIDLEEND=10


[LOCAL]
PATH=/app/dm6/dataData
#PATH=/data/dt2/xaGeneData/data
#File name pattern section :
[FILES]
Localpath=/data/dt1/dma/tstfiles
#Localpath=/data/dt4/zkb_dm6_test/temp_test
#�ο�dma-59
year=-8t
month=-4t
day=-2t
separator=;
hour=p
sequencecheck =0
#�ο�DMA-64
recordnumcheck =0
recordfmtcheck =0
#�������ݵ����Ŀ��ƣ�1��ʾ����������ENDCHECKDELAY��ENDDELAY������Ч����0��ʾ����ENDCHECKDELAY��ENDDELAY����������
forceenddump=1
#�ļ����п���ѡ��Ķ�̬������
# $year $month��$day $hour: ʱ�����
# $table :Ŀ�����
# $db : Ŀ�����ݿ���
#�ļ����м䲻���пո�
FILENAME="$db-$table_$year-$month-$day_$hour-*.txt"
#�����ļ���·�����ļ��ֽ�ģ����Խ���Ŀ¼,�����ļ�����ʹ�ù���Ŀ¼��
# ����̲�������ʱ����ͬ��������ͬһ������Ŀ¼����
# ���ر����·�����Բ��ǹ���·��
BACKUPPATH=/data/dt1/dma/backup
#BACKUPPATH=/data/dt4/zkb_dm6_test/dest_test
#�����ļ���������������������ڣ��ᱻɾ��
FILEKEEPDAYS=3

#�ɼ�����ļ��Ƿ�����Ĭ��ɾ��
#keeppickfile=1
#�����Ƿ�ֱ�ӷּ������ɼ���������Ĭ����Ҫ�ɼ�
#skippickup
#���زɼ��������ļ���Ĭ�ϱ���
#skipbackup

#�ļ����ѭ�����ڣ���
SEEKINTERVAL=60
#�ļ���ֹʱ�䣬�Ӻ������������ǰ���Ŀ����Ӧ���ڵ��Ӻ��������
ENDDELAY=3
#�ļ���������󣬽�ֹʱ���飬�Ӻ���������������������һ�ļ��ı��ش���ʱ���ӳ�
ENDCHECKDELAY=1
HEADER=*
enclosedby="\""


