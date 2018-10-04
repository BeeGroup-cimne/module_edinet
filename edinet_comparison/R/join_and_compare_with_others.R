#!/usr/bin/Rscript


#####################################
### Average and efficient users average
### * Given an input sequence file calculates its average and the efficient users average and creates a file with result  
### 
### Script parameters:
### ** companyid: string to compose hbase key
### ** month: Integer representing a YYYYMM date to compose hbase key
### ** paths['input']: Input file with records to join
### ** paths['means']: File where all the mean values depending the grouping criteria are saved
### ** paths['result_dir']: Output directory  
#####################################

## Get parameters to use later

library(RJSONIO)

h = basicJSONHandler()

f <- file("stdin")
open(f)
params <- fromJSON(readLines(f), h)

library(Rhipe)
rhinit()

criterias <- unlist(strsplit(params$criterias,","))
all_criteria_fields <- unlist(strsplit(params$all_criteria_fields,","))
mean_results<-list()
grouping<-list()
for (criteria in criterias){
	mean_results[[criteria]] <- rhread(paste(params$path_means,"/",criteria,sep=""))
	grouping[[criteria]] <- as.vector(sapply(mean_results[[criteria]],'[[',1))
}
contractId_column <- params$contractId_column
yearmonth_column <- params$yearmonth_column
number_days_column <- params$number_days_column
value_column <- params$value_column
key_column <- params$key_column
companyId <- params$companyId
triggerValue <- params$triggerValue
dailyResampling_company <- as.logical(params$dailyResampling_company)
n_reducers <- 8

select_convinient_group<-function(grouping_criterias,
		customers_per_grouping_criterias,
		min_number_of_customers){
	tf = customers_per_grouping_criterias>=min_number_of_customers
	if(sum(tf)>0){
		criteria_selected<-grouping_criterias[customers_per_grouping_criterias==min(ifelse(tf==T,customers_per_grouping_criterias,NA),na.rm=T)][1]
	} else {
		criteria_selected<-grouping_criterias[customers_per_grouping_criterias==max(customers_per_grouping_criterias,na.rm=T)][1]
	}
	return(criteria_selected)
}

select_convinient_group_multi_yearmonth<-function(customers_per_grouping_criterias,
		min_number_of_customers){
	
	#customers_per_grouping_criterias = nocust_per_group
	#min_number_of_customers=as.numeric(triggerValue)
	
	# example result for customers_per_grouping_criterias (yearmonth X every criteria combination)
	#						[,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10] [,11] [,12]
	#city                    15   36   36   36   36   36   36   36   36    36    36    36
	#criteria_1              13   13   13   13   13   13   13   13   13    13    13    13
	#criteria_1-postalCode   13   13   13   13   13   13   13   13   13    13    13    13
	#postalCode              36   36   36   36   36   36   36   36   36    10    36    36
	
	# only for cases when there is only one month of data
	if(!is.matrix(customers_per_grouping_criterias)){
	  customers_per_grouping_criterias <- matrix(customers_per_grouping_criterias,ncol=1,dimnames=list(names(customers_per_grouping_criterias),c("V1")))
	}
	# force matrix to numeric
	customers_per_grouping_criterias <- apply(customers_per_grouping_criterias,1:2,as.numeric)
	
	tf <- rowSums(customers_per_grouping_criterias>=min_number_of_customers)
	tf <- tf[!is.na(tf)]
	
	# Considering a min_number_of_customers of 15...
	#The tf matrix (not the rowSums calculation) is the following...
	#                       [,1]  [,2]  [,3]  [,4]  [,5]  [,6]  [,7]  [,8]  [,9] [,10] [,11] [,12]
	#city                   TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE
	#criteria_1            FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE
	#criteria_1-postalCode FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE
	#postalCode             TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE FALSE  TRUE  TRUE
	
	minmat <- rowSums(
	  abs(
	    ifelse(customers_per_grouping_criterias - min_number_of_customers < 0, 
	           (customers_per_grouping_criterias - min_number_of_customers) * 10,
	           customers_per_grouping_criterias - min_number_of_customers)
	  )
	)
	minmat <- minmat[!is.na(minmat)]
	
	# And the minmat matrix (not the rowSums calculation) is the following...
	#                         [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10] [,11] [,12]
	#city                     0   21   21   21   21   21   21   21   21    21    21    21
	#criteria_1              20   20   20   20   20   20   20   20   20    20    20    20
	#criteria_1-postalCode   20   20   20   20   20   20   20   20   20    20    20    20
	#postalCode              21   21   21   21   21   21   21   21   21    50    21    21
	
	if( ncol(customers_per_grouping_criterias) %in% tf ){
	  criteria_selected<-names( minmat[which( minmat %in% min(minmat[tf==ncol(customers_per_grouping_criterias) ],na.rm=T) )][1] )
	} else {
	  max_tf = tf[which(tf %in% max(tf))]
	  criteria_selected<-names( minmat[which( minmat %in% min(minmat[tf==max_tf ],na.rm=T) )][1] )
	}
	
	return(criteria_selected)
}


numberOfDays <- function(date) {
	m <- format(date, format="%m")
	while (format(date, format="%m") == m) {
		date <- date + 1
	}
	return(as.integer(format(date - 1, format="%d")))
}

## Create result file to be dumped in HBase through Hive
## We should join information and operate with values on first input 
## ---------- It is a text tab delimited with costumerid, value, zip
## Our job will read each record and operate with value (depending on zip code)
## ---------- Outputs a text file with columns: costumerid, value, mean_efi, mean_all, diff_efi, diff_all
## It would be a map only job

joinMap <- expression({
			line <- strsplit(unlist(map.values),'\t')
			
			mapply(function(line){
						# Yearmonth and contractId of this line
						yearmonth<-as.character(line[yearmonth_column])
						contractId <- line[contractId_column]
						
						# Number of days (Real and theoric for this month)
						real_number_days <- as.numeric(line[number_days_column])
						theoric_number_days <- numberOfDays(as.Date(strptime(paste(yearmonth, "28", sep=''),format = "%Y%m%d")))
						
						# Consumption and degree days value of this yearmonth and contractId
						consumption <- as.numeric(line[value_column])
						
						# Criteria items for this contractId
						criteria_items <- line[key_column:length(line)]
						
						# KEY definition
						key_reducer <- prod(strtoi(charToRaw(contractId)),na.rm=T)
						if(nchar(key_reducer)>5){
							a<-nchar(key_reducer)-5
							b<-nchar(key_reducer)-2
						} else {
							a<-1
							b<-nchar(key_reducer)
						}
						whichReducer<- as.integer(substr(key_reducer,a,b)) %% n_reducers
						
						
						rhcollect(c(whichReducer,contractId),c(yearmonth,consumption,real_number_days,theoric_number_days,criteria_items))
						
					}, line, SIMPLIFY=FALSE)
			
		})

#joinReducer_test<-expression(
#	reduce={
#		df_ini <- do.call("rbind",reduce.values)
#		rhcollect(reduce.key[2],df_ini)
#	}
#)
#
#joining_test <- rhwatch(map=joinMap, reduce=joinReducer_test, partitioner=list(lims=1,type='string'),
#		input=rhfmt(params$input,type='text'), 
#		output=params$paths$result_dir,
#		read=FALSE, noeval=FALSE,
#		mapred=list(mapred.field.separator=",", mapred.textoutputformat.usekey=FALSE,mapred.reduce.tasks=n_reducers)
#)
#prova<-rhread(params$paths$result_dir)
#reduce.key<-prova[[1]][[1]]
#df<-prova[[1]][[2]]

joinReducer<-expression(
		
		reduce={
			
			df <- do.call("rbind",reduce.values)
			
			#for(i in 1:45){
			#i=35
			#df<-prova[[i]][[2]]
			#reduce.key<-prova[[i]][[1]]
			
			if (nrow(df)==1){
				df <- data.frame("ym"=as.integer(df[,1]),"consumption"=as.numeric(df[,2]),"real_days"=as.integer(df[,3]),"theoric_days"=as.integer(df[,4]),t(df[,5:ncol(df)]))
			}else{
				df <- data.frame("ym"=as.integer(df[,1]),"consumption"=as.numeric(df[,2]),"real_days"=as.integer(df[,3]),"theoric_days"=as.integer(df[,4]),df[,5:ncol(df)])
			}
			colnames(df)[5:ncol(df)] = all_criteria_fields
			
			# Grouping criteria to select for this contractId
			nocust_per_group <- 
					mapply(function(ym_aux){
								mapply(function(criteria){
											key_summary <- paste(ym_aux,gsub(", ","-",toString(mapply(function(x) df[df$ym==ym_aux,x],unlist(strsplit(criteria,"-"))))),sep="~")
											nocust <- mean_results[[criteria]][[match(key_summary,grouping[[criteria]])]][[2]][2]
											if(is.null(nocust)) NA else nocust
										},criterias)
							},df$ym)
			
			if(length(criterias)==1){
				nocust_per_group <- data.frame(nocust_per_group)
				colnames(nocust_per_group) <- criterias[1]
				nocust_per_group <- t(nocust_per_group)
			}
			
			criteria_selected<-suppressWarnings(select_convinient_group_multi_yearmonth(nocust_per_group,as.numeric(triggerValue)))
			
			# Only emit result if there is a valid criteria to select
			if(!is.na(criteria_selected)){
				
				mapply( function(ym){
						
						key <- sprintf("%s~%s~%s", ym, as.character(companyId), reduce.key[2])
						
						key_summary <- paste(ym,gsub(", ","-",toString(mapply(function(x) df[1,x],unlist(strsplit(criteria_selected,"-"))))),sep="~")
						
						# Select the comparative values for this contractId and yearmonth
						comparative <- mean_results[[criteria_selected]][[match(key_summary,grouping[[criteria_selected]])]][[2]]
						
						if (!is.null(comparative)){
							mean_all <- as.numeric(comparative[1])
							nocust_all <- as.integer(comparative[2])
							mean_eff <- as.numeric(comparative[3])
							nocust_eff <- as.integer(comparative[4])
							real_days <- df$real_days[df$ym==ym]
							theoric_days <- df$theoric_days[df$ym==ym]
							if (real_days<theoric_days){
								if (real_days==1  && dailyResampling_company==FALSE){
									real_days <- theoric_days	
								}
								mean_all <- mean_all * (real_days / theoric_days)
								mean_eff <- mean_eff * (real_days / theoric_days)
							}
							consumption <- df$consumption[df$ym==ym]
							
							# Difference calculation considering the contractId consumption and his similars
							diff_eff <- ifelse(mean_eff!=0,((consumption-mean_eff)/mean_eff)*100,0)
							diff_all <- ifelse(mean_all!=0,((consumption-mean_all)/mean_all)*100,0)
							
							rhcollect(NULL, c(key,consumption,mean_eff,mean_all,diff_eff,diff_all,nocust_eff,nocust_all,criteria_selected,real_days))
							#c(key,consumption,mean_eff,mean_all,diff_eff,diff_all,nocust_eff,nocust_all,criteria_selected,real_days)
						
						}
				
					}, df$ym, SIMPLIFY=FALSE)
			#}, df$ym)
			}
			
		}

)

joining <- rhwatch(map=joinMap, reduce=joinReducer, partitioner=list(lims=1,type='string'),
		input=rhfmt(params$input,type='text'), 
		output=rhfmt(params$paths$result_dir,type='text'),
		read=FALSE, noeval=TRUE,
		mapred=list(mapred.field.separator=",", mapred.textoutputformat.usekey=FALSE,mapred.reduce.tasks=n_reducers)
)

job <- rhex(joining, async=TRUE)
status <- rhstatus(job)
print('Printing rhstatus')
toJSON(status, collapse='', .escapeEscapes = FALSE)




rhclean()

#joinMap_old_resource_ONLYMAPPER <- expression({
#			line <- strsplit(unlist(map.values),'\t')
#			
#			mapply(function(line){
#						# Yearmonth and contractId of this line
#						yearmonth<-as.character(line[yearmonth_column])
#						contractId <- line[contractId_column]
#						
#						# Number of days (Real and theoric for this month)
#						real_number_days <- as.numeric(line[number_days_column])
#						theoric_number_days <- numberOfDays(as.Date(strptime(paste(yearmonth, "28", sep=''),format = "%Y%m%d")))
#						
#						# Consumption and degree days value of this yearmonth and contractId
#						consumption <- as.numeric(line[value_column])
#						
#						# Possible grouping criterias for this contractId
#						keys_summary <- lapply(criterias,function(criteria){
#									#line[ which(all_criteria_fields %in% unlist(strsplit(criteria,"-"))) + (key_column-1) ] ## bad order if more than one criteria
#									mapply(function(x) line[which(all_criteria_fields %in% x) + (key_column-1) ],unlist(strsplit(criteria,"-")))
#								})
#						
#						# Selected grouping criteria for this contractId
#						nocust_per_group <- lapply(criterias,function(criteria){
#									key_summary <- paste(yearmonth,gsub(", ","-",toString( mapply(function(x) line[which(all_criteria_fields %in% x) + (key_column-1) ],unlist(strsplit(criteria,"-"))) )),sep="~")
#									mean_results[[criteria]][[match(key_summary,grouping[[criteria]])]][[2]][2]
#								})
#						criteria_selected<-select_convinient_group(criterias,unlist(nocust_per_group),as.numeric(triggerValue))
#						key_summary <- paste(yearmonth,gsub(", ","-",toString( mapply(function(x) line[which(all_criteria_fields %in% x) + (key_column-1) ],unlist(strsplit(criteria,"-"))) )),sep="~")
#						
#						# Select the comparative values for this contractId and yearmonth 
#						comparative <- mean_results[[criteria_selected]][[match(key_summary,grouping[[criteria_selected]])]][[2]]
#						mean_all <- as.numeric(comparative[1])
#						nocust_all <- as.integer(comparative[2])
#						mean_eff <- as.numeric(comparative[3])
#						nocust_eff <- as.integer(comparative[4])
#						if (real_number_days < theoric_number_days){
#							if (real_number_days==1  && dailyResampling_company==FALSE){
#								real_number_days <- theoric_number_days	
#							}
#							mean_all <- mean_all * (real_number_days / theoric_number_days)
#							mean_eff <- mean_eff * (real_number_days / theoric_number_days)
#						}
#						
#						# Difference calculation considering the contractId consumption and his similars
#						diff_eff <- ifelse(mean_eff!=0,((consumption-mean_eff)/mean_eff)*100,0)
#						diff_all <- ifelse(mean_all!=0,((consumption-mean_all)/mean_all)*100,0)
#						
#						# KEY definition
#						key <- sprintf("%s~%s~%s", yearmonth, as.character(companyId), contractId)
#						
#						rhcollect(NULL, c(key,consumption,mean_eff,mean_all,diff_eff,diff_all,nocust_eff,nocust_all,criteria_selected,real_number_days))
#						
#					}, line, SIMPLIFY=FALSE)
#		})
#
#joining <- rhwatch(map=joinMap, reduce=0, 
#		input=rhfmt(params$input,type='text'), 
#		output=rhfmt(params$paths$result_dir,type='text'),
#		read=FALSE, noeval=TRUE,
#		mapred=list(mapred.field.separator=",", mapred.textoutputformat.usekey=FALSE)
#)
#
#job <- rhex(joining, async=TRUE)
#status <- rhstatus(job)
#print('Printing rhstatus')
#toJSON(status, collapse='', .escapeEscapes = FALSE)
