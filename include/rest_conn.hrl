-record(spark_rest_config,{       
	 idMap    = [],
	 spark_api_endpoint = <<"">>,
   spark_app_id,
   spark_brand_id,
   spark_client_secret = <<"">>,
   spark_create_oauth_accesstoken =
              <<"/brandId/{brandId}/oauth2/accesstoken/application/{applicationId}">>,
   auth_profile_miniProfile =
              <<"/brandId/{brandId}/profile/attributeset/miniProfile/{targetMemberId}">>,
   profile_memberstatus =
              <<"/brandId/{brandId}/application/{applicationId}/member/{memberId}/status">>,
   community2brandId = 
              [{spark, <<"1">> , <<"1001">> },
               {jdate, <<"3">> , <<"1003">> },
               {cupid, <<"10">> , <<"1015">> },
               {bbw, <<"23">> , <<"90410">> },
               {blacksingle, <<"24">> , <<"90510">> }]
}).
