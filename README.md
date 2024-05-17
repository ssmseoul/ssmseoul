# terminal
cd ~/work/ssmseoul
git pull

# terminal
docker-compose pull
docker-compose up -d

# terminal
docker ps -a

# terminal 
docker rm -f `docker ps -aq`

# terminal
docker-compose pull
docker-compose up -d

# terminal
docker-compose logs notebook | grep 8888

# 코어 스파크 라이브러리를 임포트 합니다
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession
    .builder
    .appName("Data Engineer Training Course")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .getOrCreate()
)

# 노트북에서 테이블 형태로 데이터 프레임 출력을 위한 설정을 합니다
from IPython.display import display, display_pretty, clear_output, JSON
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # display enabled
spark.conf.set("spark.sql.repl.eagerEval.truncate", 100) # display output columns size
spark

user25 = spark.read.parquet("user/20201025")
user25.printSchema()
user25.show(truncate=False)
display(user25)

user25.createOrReplaceTempView("user25")
purchase25.createOrReplaceTempView("purchase25")
access25.createOrReplaceTempView("access25")
spark.sql("show tables '*25'")

u_signup_condition = "<10월 25일자에 등록된 유저만 포함되는 조건을 작성합니다>"
user = spark.sql("select u_id, u_name, u_gender from user25").where(u_signup_condition)
user.createOrReplaceTempView("user")
display(user)

p_time_condition = "<10월 25일자에 발생한 매출만 포함되는 조건을 작성합니다>"
purchase = spark.sql("select from_unixtime(p_time) as p_time, p_uid, p_id, p_name, p_amount from purchase25").where(p_time_condition)
purchase.createOrReplaceTempView("purchase")
display(purchase)

access = spark.sql("select a_id, a_tag, a_timestamp, a_uid from access25")
access.createOrReplaceTempView("access")
display(access)

spark.sql("show tables")

spark.sql("describe user")
spark.sql("select * from user")
# whereCondition = "<성별을 구별하는 조건을 작성하세요>"
# spark.sql("select * from user").where(whereCondition)

spark.sql("describe purchase")
spark.sql("select * from purchase")
# selectClause = "<금액을 필터하는 조건을 작성하세요>"
# spark.sql(selectClause)

spark.sql("describe access")
spark.sql("select * from access")
# groupByClause="<로그인/아웃 컬럼을 기준으로 집계하는 구문을 작성하세요>"
# spark.sql(groupByClause)

display(access)
# distinctAccessUser = "select <고객수 집계함수> as DAU from access"
# dau = spark.sql(distinctAccessUser)
# display(dau)
# v_dau = dau.collect()[0]["DAU"]

display(purchase)
# distinctPayingUser = "<구매 고객수 집계함수>"
# pu = spark.sql(distinctPayingUser)
# display(pu)
# v_pu = pu.collect()[0]["PU"]

display(purchase)
# sumOfDailyRevenue = "<일 별 구매금액 집계함수>"
# dr = spark.sql(sumOfDailyRevenue)
# display(dr)
# v_dr = dr.collect()[0]["DR"]

# print("ARPU : {}".format(<유저당 매출 금액 계산식>))
print("+------------------+")
print("|             ARPU |")
print("+------------------+")
print("|        {} |".format(v_dr / v_dau))
print("+------------------+")

# print("ARPPU : {}".format(<구매유저 당 매출 금액 계산식>))
print("+------------------+")
print("|            ARPPU |")
print("+------------------+")
print("|        {} |".format(v_dr / v_pu))
print("+------------------+")

