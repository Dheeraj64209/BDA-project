# Official Hadoop image + the compiled MapReduce job.
# The Hadoop image ships only a Java runtime, so the job is compiled in a JDK stage first.
FROM apache/hadoop:3.4.1 AS hadoop

FROM eclipse-temurin:8-jdk AS build
COPY --from=hadoop /opt/hadoop/share/hadoop /hadoop
COPY ReturnRateJob.java run_job.sh /src/
RUN mkdir /classes \
 && javac -cp "/hadoop/common/*:/hadoop/common/lib/*:/hadoop/mapreduce/*" -d /classes /src/ReturnRateJob.java \
 && jar cf /src/return-rate.jar -C /classes . \
 && sed -i 's/\r$//' /src/run_job.sh

FROM apache/hadoop:3.4.1
COPY --from=build /src/return-rate.jar /src/run_job.sh /opt/jobs/
