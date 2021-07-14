package ru.otus.bigdataml.ht3

import org.apache.spark.sql.DataFrame

case class DataColumns(df:DataFrame) {
  val categorical=Array("instanceId_objectType_num", "audit_resourceType_num", "metadata_ownerType_num", "membership_status_num", "log_ownerId","log_authorId")

  val categoricalClassesNum=Array(3, 5, 2, 9, 6, 7)

  val toExclude=Array("target","instanceId_objectType","audit_resourceType","metadata_ownerType","membership_status","instanceId_userId", "instanceId_objectId","audit_clientType","audit_timestamp","audit_timePassed",
    "audit_experiment","metadata_ownerId","metadata_createdAt","metadata_authorId","metadata_applicationId",
    "metadata_platform","metadata_options","relationsMask","membership_statusUpdateDate","membership_joinDate","membership_joinRequestDate",
    "owner_create_date","owner_birth_date", "feedback","objectId","date","createdTime","auditedTime")
  
  val forProducer = df.columns diff toExclude

  val numerical=forProducer diff categorical

  val categoricalClasses = (categorical zip categoricalClassesNum)

  val categoricalEncoded=categorical.map(c=>s"${c}_enc")

  
}
