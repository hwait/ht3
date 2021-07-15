package ru.otus.bigdataml.ht3

/*
*
*  Данные об именах столбцов и преобразования их. 
*
*/

trait DataColumns {

  private val columns=Array("instanceId_userId","instanceId_objectType","instanceId_objectId","audit_pos","audit_clientType","audit_timestamp","audit_timePassed","audit_experiment","audit_resourceType","metadata_ownerId","metadata_ownerType","metadata_createdAt","metadata_authorId","metadata_applicationId","metadata_numCompanions","metadata_numPhotos","metadata_numPolls","metadata_numSymbols","metadata_numTokens","metadata_numVideos","metadata_platform","metadata_totalVideoLength","metadata_options","relationsMask","userOwnerCounters_USER_FEED_REMOVE","userOwnerCounters_USER_PROFILE_VIEW","userOwnerCounters_VOTE_POLL","userOwnerCounters_USER_SEND_MESSAGE","userOwnerCounters_USER_DELETE_MESSAGE","userOwnerCounters_USER_INTERNAL_LIKE","userOwnerCounters_USER_INTERNAL_UNLIKE","userOwnerCounters_USER_STATUS_COMMENT_CREATE","userOwnerCounters_PHOTO_COMMENT_CREATE","userOwnerCounters_MOVIE_COMMENT_CREATE","userOwnerCounters_USER_PHOTO_ALBUM_COMMENT_CREATE","userOwnerCounters_COMMENT_INTERNAL_LIKE","userOwnerCounters_USER_FORUM_MESSAGE_CREATE","userOwnerCounters_PHOTO_MARK_CREATE","userOwnerCounters_PHOTO_VIEW","userOwnerCounters_PHOTO_PIN_BATCH_CREATE","userOwnerCounters_PHOTO_PIN_UPDATE","userOwnerCounters_USER_PRESENT_SEND","userOwnerCounters_UNKNOWN","userOwnerCounters_CREATE_TOPIC","userOwnerCounters_CREATE_IMAGE","userOwnerCounters_CREATE_MOVIE","userOwnerCounters_CREATE_COMMENT","userOwnerCounters_CREATE_LIKE","userOwnerCounters_TEXT","userOwnerCounters_IMAGE","userOwnerCounters_VIDEO","ownerUserCounters_USER_FEED_REMOVE","ownerUserCounters_USER_PROFILE_VIEW","ownerUserCounters_VOTE_POLL","ownerUserCounters_USER_SEND_MESSAGE","ownerUserCounters_USER_DELETE_MESSAGE","ownerUserCounters_USER_INTERNAL_LIKE","ownerUserCounters_USER_INTERNAL_UNLIKE","ownerUserCounters_USER_STATUS_COMMENT_CREATE","ownerUserCounters_PHOTO_COMMENT_CREATE","ownerUserCounters_MOVIE_COMMENT_CREATE","ownerUserCounters_USER_PHOTO_ALBUM_COMMENT_CREATE","ownerUserCounters_COMMENT_INTERNAL_LIKE","ownerUserCounters_USER_FORUM_MESSAGE_CREATE","ownerUserCounters_PHOTO_MARK_CREATE","ownerUserCounters_PHOTO_VIEW","ownerUserCounters_PHOTO_PIN_BATCH_CREATE","ownerUserCounters_PHOTO_PIN_UPDATE","ownerUserCounters_USER_PRESENT_SEND","ownerUserCounters_UNKNOWN","ownerUserCounters_CREATE_TOPIC","ownerUserCounters_CREATE_IMAGE","ownerUserCounters_CREATE_MOVIE","ownerUserCounters_CREATE_COMMENT","ownerUserCounters_CREATE_LIKE","ownerUserCounters_TEXT","ownerUserCounters_IMAGE","ownerUserCounters_VIDEO","membership_status","membership_statusUpdateDate","membership_joinDate","membership_joinRequestDate","owner_create_date","owner_birth_date","owner_gender","owner_status","owner_ID_country","owner_ID_Location","owner_is_active","owner_is_deleted","owner_is_abused","owner_is_activated","owner_change_datime","owner_is_semiactivated","owner_region","user_create_date","user_birth_date","user_gender","user_status","user_ID_country","user_ID_Location","user_is_active","user_is_deleted","user_is_abused","user_is_activated","user_change_datime","user_is_semiactivated","user_region","feedback","objectId","auditweights_ageMs","auditweights_closed","auditweights_ctr_gender","auditweights_ctr_high","auditweights_ctr_negative","auditweights_dailyRecency","auditweights_feedOwner_RECOMMENDED_GROUP","auditweights_feedStats","auditweights_friendCommentFeeds","auditweights_friendCommenters","auditweights_friendLikes","auditweights_friendLikes_actors","auditweights_hasDetectedText","auditweights_hasText","auditweights_isPymk","auditweights_isRandom","auditweights_likersFeedStats_hyper","auditweights_likersSvd_prelaunch_hyper","auditweights_matrix","auditweights_notOriginalPhoto","auditweights_numDislikes","auditweights_numLikes","auditweights_numShows","auditweights_onlineVideo","auditweights_partAge","auditweights_partCtr","auditweights_partSvd","auditweights_processedVideo","auditweights_relationMasks","auditweights_source_LIVE_TOP","auditweights_source_MOVIE_TOP","auditweights_svd_prelaunch","auditweights_svd_spark","auditweights_userAge","auditweights_userOwner_CREATE_COMMENT","auditweights_userOwner_CREATE_IMAGE","auditweights_userOwner_CREATE_LIKE","auditweights_userOwner_IMAGE","auditweights_userOwner_MOVIE_COMMENT_CREATE","auditweights_userOwner_PHOTO_COMMENT_CREATE","auditweights_userOwner_PHOTO_MARK_CREATE","auditweights_userOwner_PHOTO_VIEW","auditweights_userOwner_TEXT","auditweights_userOwner_UNKNOWN","auditweights_userOwner_USER_DELETE_MESSAGE","auditweights_userOwner_USER_FEED_REMOVE","auditweights_userOwner_USER_FORUM_MESSAGE_CREATE","auditweights_userOwner_USER_INTERNAL_LIKE","auditweights_userOwner_USER_INTERNAL_UNLIKE","auditweights_userOwner_USER_PRESENT_SEND","auditweights_userOwner_USER_PROFILE_VIEW","auditweights_userOwner_USER_SEND_MESSAGE","auditweights_userOwner_USER_STATUS_COMMENT_CREATE","auditweights_userOwner_VIDEO","auditweights_userOwner_VOTE_POLL","auditweights_x_ActorsRelations","auditweights_likersSvd_spark_hyper","auditweights_source_PROMO","date","target","createdTime","auditedTime","timeDelta","createdHour","auditedHour","log_ownerId","log_authorId","instanceId_objectType_num","audit_resourceType_num","metadata_ownerType_num","membership_status_num")

  private val categoricalColumns=Array("instanceId_objectType_num", "audit_resourceType_num", "metadata_ownerType_num", "membership_status_num", "log_ownerId","log_authorId")

  // Число классов для категориальных признаков
  private val categoricalClassesNum=Array(3, 5, 2, 9, 6, 7) 

  private val columnsToExclude=Array("target","instanceId_objectType","audit_resourceType","metadata_ownerType","membership_status","instanceId_userId", 
  "instanceId_objectId","audit_clientType", "audit_timestamp","audit_timePassed", "audit_experiment","metadata_ownerId","metadata_createdAt",
  "metadata_authorId","metadata_applicationId","metadata_platform","metadata_options","relationsMask","membership_statusUpdateDate",
  "membership_joinDate","membership_joinRequestDate", "owner_create_date","owner_birth_date", "feedback","objectId","date","createdTime","auditedTime")
  
  val exportColumns = columns diff columnsToExclude

  val numericalColumns=exportColumns diff categoricalColumns

  // Данное объединение потребуется для формирования энкодеров.
  val categoricalColumnsWithClassesNum = (categoricalColumns zip categoricalClassesNum)

  val categoricalEncodedColumns=categoricalColumns.map(c=>s"${c}_enc")

  val numericalAssemblerOutputColumn = "features_numerical"
  val normalizerOutputColumn = "features_norm"
  val scalerOutputColumn = "features_scaled"
  val timeColumn = "auditedTime"
  val featuresColumn = "features"
  val targetColumn = "target"
}
