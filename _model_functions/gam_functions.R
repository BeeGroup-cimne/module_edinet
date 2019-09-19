###  Load libraries  ----
load_libraries <- function(){
    libs <- c("tidyr","dplyr","splines","data.table","mgcv", "normwhn.test","oce",
             "padr","zoo","pracma","GA","scales","hwwntest","readr",
             "clusterCrit","mclust","pastecs")
    for(l in libs){
        require(l,character.only=T)
        tryCatch( require(l,character.only=T),
            warning=function(e){
                install.packages(l)
                require(l,character.only=T)
            }
        )
    }
}






### Prediction functions ----

GaussianMixtureModel_prediction <- function(model, new_data=NULL, value_column=NULL, timezone="UTC") {
    Sys.setenv(TZ=timezone)
    message('starting the clustering prediction')
    if (is.null(value_column)){ value_column <- model$clustering$value_column }
    new_data$date <- as.Date(new_data$time,tz=timezone)
    df_agg<-aggregate(as.numeric(new_data[,value_column]),by=list(
        strftime(strptime(new_data$time,format="%Y-%m-%d %H:%M:%S",tz=timezone),format = "%Y-%m-%d %H",tz=timezone)),
        FUN=sum
    )
    df_agg<-data.frame(
        "time"= as.POSIXct(as.character(df_agg$Group.1),format="%Y-%m-%d %H",tz=timezone),
        "day"=as.Date(as.POSIXct(as.character(df_agg$Group.1),format="%Y-%m-%d %H",tz=timezone),tz=timezone),
        "value"=df_agg$x,
        "dayhour"=sprintf("%02i:00",as.integer(substr(df_agg$Group.1,12,13))),
        stringsAsFactors = F
    )

    message('calculate mean and sd to normalize')
    mean_sd_to_normalize <- mapply(function(i) c("mean"=mean(model$clustering$training_init[,i],na.rm=T),"sd"=sd(model$clustering$training_init[,i],na.rm=T)),
                                 1:ncol(model$clustering$training_init))
    message('preparing spread')
    df_agg <- df_agg[!duplicated(df_agg$time),]
    df_spread<-spread(df_agg[c("day","value","dayhour")],"dayhour","value")
    days <- df_spread[,1]
    df_spread<-df_spread[,-1]
    # percentage of consumption in the day
    df_spread_norm_pre<- normalize_perc_cons_day(df_spread)
    # znorm
    df_spread_norm<- normalize_znorm(df_spread_norm_pre,mean_sd_to_normalize)
    #df_spread_norm<- normalize_znorm(df_spread,spec_mean_sd = mean_sd_to_normalize)

    # Generate the final spreated dataframe normalized
    complete_cases <- complete.cases(df_spread_norm)
    new_data_norm<- df_spread_norm[complete_cases,]
    message(paste('complete cases: n ',nrow(new_data_norm)))
    message(paste('test: n ',ncol(new_data_norm)))
    day_clusters = predict(model$clustering, newdata=new_data_norm)$classification
    message('prediction results')
    results_prediction <- data.frame(
        "date"=days[complete_cases],
        "s"=as.character(day_clusters)
    )

    results <- merge(df_agg[,c("day","time","value","dayhour")],results_prediction,by.x="day",by.y="date",stringsAsFactors=F)
    colnames(results) <- c("day","time","value","dayhour","s")
    return(results)
}

predict_mod_by_s<-function(model, newdata){
    m <- model$model
    prediction <- unlist(lapply( unique(newdata$date), function(d){
        ad <- newdata[newdata$date==d,]
        s <- as.character(unique(ad$s))
        m_ <- m[[s]]

        # Tune the inputs (Low pass filters, thresholds, autoregressive orders, ...)
        ad <- tune_inputs(ad, m_$value_column ,m_$n, m_$m, m_$alpha_t, m_$alpha_g, m_$alpha_w, m_$tbal, m_$p)

        pred<-rep(0,24)
        for (i in 1:24){
            pred[i] <- predict(m_,ad[i,])
            for (j in 1:(24-i)){
                if((i+j)<=24){
                    ad[i+j,paste0(value_column,"_lag",j)]<-pred[i]
                }
            }
        }
        pred
    }))

    msd = list()
    for (i in names(m)){
        m_ = m[[i]]
        msd[[i]] <- aggregate(m_$residuals, by=list("h"=as.numeric(as.character(m_$model$dayhour_f))),sd)
    }
    sd_prediction <- unlist(lapply( unique(newdata$date), function(d){
        d = unique(newdata$date)[18]
        ad <- newdata[newdata$date==d,]
        s <- as.character(unique(ad$s))
        msd[[s]]$x
    }))
    return(list("pred"=prediction, "sd"=sd_prediction))
}

predict_mod_all<-function(model, newdata){

    m <- model$model
    # Tune the inputs (Low pass filters, thresholds, autoregressive orders, ...)
  newdata <- tune_inputs(newdata, m$value_column, m$n, m$m, m$alpha_t, m$alpha_g, m$alpha_w, m$tbal, m$p)

  prediction <- exp(unlist(lapply( sort(unique(newdata$date)), function(d){
    ad <- newdata[newdata$date==d,]
    daylen <- nrow(ad)
    pred<-rep(0,daylen)
    for (i in 1:daylen){
      pred[i] <- predict(m,ad[i,])
       for (j in 1:(daylen-i)){
        if((i+j)<=daylen){
          ad[i+j,paste0(m$value_column,"_lag",j)] <- pred[i]
          ad[i+j,paste0(m$value_column,"_lag",j,"_s",ad$s[1])] <- pred[i]
          ad[i+j,paste0(m$value_column,"_occ_lag",j)] <- if(ad$occupancy_tf[i]==1){pred[i]}else{log(0.01)}
          ad[i+j,paste0(m$value_column,"_occ_lag",j,'_s',ad$s[1])] <- if(ad$occupancy_tf[i]==1){pred[i]}else{log(0.01)}
        }
      }
    }
    pred
  })))
  return(list("pred"=prediction))
}




### Data wrangling functions ----


prepare_dataframe <- function(model, df, value_column, n_lags, lat=NULL, lon=NULL, timezone="UTC", str_val=NULL){
    Sys.setenv(TZ=timezone)
    df$date <- as.Date(df$time,tz=timezone)
    df_centroids_raw<-aggregate(df$value, by=list(df$dayhour,df$s),FUN=mean)
    colnames(df_centroids_raw)<-c("dayhour","s","value")
    df$centroid <- mapply(function(i){
        a<-(df_centroids_raw$value[df_centroids_raw$dayhour == df$dayhour[i] & df_centroids_raw$s == df$s[i]])
        if(is.finite(a)){
            a
        }else{
            NA
        }
        },1:nrow(df)
    )
    #Normalized version (0-1) Occupancy rate
    df_centroids_norm <-df_centroids_raw
    for (s_ in unique(df_centroids_norm$s)){
        df_centroids_norm$value[df_centroids_norm$s==s_] <- normalize_range_int(x = df_centroids_norm$value[df_centroids_norm$s==s_],
                                                    inf=0, sup = 100, threshold_for_min = 1)
        # Consumptions of less than 1 kWh per timestep, are considered as the low-range consumption after the normalization
    }
    df$occupancy <- mapply(function(i){
        a<-(df_centroids_norm$value[df_centroids_norm$dayhour == df$dayhour[i] & df_centroids_norm$s == df$s[i]])
        if(is.finite(a)){a}else{NA}
        },1:nrow(df)
    )
    df_case <- data.frame(df$time, df$date, df[,value_column], df$centroid, hour(df$time), df$occupancy, df$clustering_values,
        strftime(df$time,format = "%u"), df$s, df$temperature, df$windSpeed, df$GHI, df$windBearing, stringsAsFactors = T
    )

    names(df_case) <- c("time", "date", value_column, "centroid", "dayhour_f", "occupancy", "clustering_values", "dayweek_f", "s",
    'temperature', 'windSpeed', 'GHI', 'windDirection')
    # Detect unique seasonalities and seosonalities related with 0 consumption
    unique_str_changes <- as.character(unique(df_case$s))[!is.na(as.character(unique(df_case$s)))]
    s_zero <- NULL
    for (s_ in unique_str_changes){
        if (sum(df_centroids_raw[df_centroids_raw$s==s_,"value"])==0) s_zero <- c(s_zero,s_)
    }

    # Consumption transformations
    df_case[,paste0("real_",value_column)] <- df_case[,value_column]
    df_case[,value_column] <- ifelse(df_case[,value_column]>=0, df_case[,value_column],NA)
    df_case <- df_case[!is.na(df_case[,value_column]),]
    # Real consumption considered in lagged consumptions
    df_case$for_lagged_calculations <- log(ifelse(df_case[,value_column]>0, df_case[,value_column],0.01))
    df_case[,value_column] <- log(ifelse(df_case[,value_column]>0, df_case[,value_column],0.01))

    # Lagged consumption values
    for (i in 1:n_lags){
        df_case[,paste0(value_column,"_lag",i)]<-shift(df_case$for_lagged_calculations,i)
        # Assign zeros when centroid is zero
        df_case[df_case$s %in% s_zero,paste0(value_column,"_lag",i)]<-log(0.01)
        # Assign the centroid daily load curve as the lagged consumption when there is a change in the seasonality
        #
        #shifted_s <- as.character(shift(df_case$s,i))
        #shifted_s[is.na(shifted_s)] <- as.character(max(as.numeric(unique(shifted_s)),na.rm=T)+1)
        #shifted_s <- as.factor(na.fill(as.character(shifted_s), "extend"))
        # for (m in (n_lags+2):nrow(df_case)){
        #   if (m>1 & i<24){
        #     if (shifted_s[m] != shifted_s[m-1]){
        #       ctroid <- log(ifelse(df_centroids[df_centroids$s==shifted_s[m],"value"]>0, df_centroids[df_centroids$s==shifted_s[m],"value"], 0.01))
        #       if (length(ctroid)>0){
        #         df_case[(m-i):min(m-1,nrow(df_case)),paste0(value_column,"_lag",i)] <- tail(ctroid,min(i,nrow(df_case)-m))
        #         if((m+i)<=nrow(df_case)){
        #           df_case[(m):min(m+24-i-1,nrow(df_case)),paste0(value_column,"_lag",i)] <- head(ctroid,min(24-i,nrow(df_case)-m))
        #         }
        #       }
        #     }
        #   }
        # }
        for (l in unique_str_changes){
            df_case[,paste0(value_column,"_lag",i,"_s",l)]<-ifelse(df$s==l,df_case[,paste0(value_column,"_lag",i)],log(0.01))
        }
    }

    # Moving average of the consumption
    for (i in 2:12){
        df_case[,paste0(value_column,"_ma",i)] <- rollmean(shift(df_case[,paste0("real_",value_column)], 1, fill=0),
                                       k=i, fill = c(0,0,i), align = "right", na.pad=T)
        df_case[,paste0(value_column,"_ma",i)] <- log(ifelse(df_case[,paste0(value_column,"_ma",i)]>0,
                                         df_case[,paste0(value_column,"_ma",i)], 0.1) )
    }

    # Calendar features
    df_case$dayhour <- as.numeric(as.character(df_case$dayhour_f))
    df_case$dayhour_f <- as.factor(sprintf("%02i",as.numeric(as.character(df_case$dayhour_f))))
    for (l in unique_str_changes){
        df_case[,paste0("dayhour_f_s",l)] <- as.factor(ifelse(df_case$s==l,sprintf("%02i",as.numeric(as.character(df_case$dayhour_f))),
                                          "NA"))
    }
    df_case$dayweek <- as.integer(as.character(df_case$dayweek_f))
    for (l in unique_str_changes){
        df_case[,paste0("dayweek_f_s",l)] <- as.factor(ifelse(df_case$s==l,sprintf("%02i",as.numeric(as.character(df_case$dayweek_f))),
                                          "NA"))
    }
    df_case$weekhour <- (df_case$dayhour)+((df_case$dayweek-1)*24)

    # Weather features
    df_case$windDirection <- ifelse(is.finite(df_case$windDirection), df_case$windDirection, -99)
    df_case$windDirection_f <- as.factor(as.character(cut(ifelse(df_case$windDirection>=(360-22.5),df_case$windDirection-360,df_case$windDirection),c(-100,seq(-22.5,360-22.5,45)),
                           labels = c("NA","N","NE","E","SE","S","SO","O","NO"))))

    timeutc <- as.POSIXct(strptime(df$time, format = "%Y-%m-%d %H:%M:%S", tz =timezone), tz = timezone)
    timeutc <- as.POSIXlt(timeutc, tz = "UTC")

    df_case$sunElevation <- sunAngle(timeutc, latitude = lat, longitude = lon)$altitude
    df_case$sunAzimuth <- sunAngle(timeutc, latitude = lat, longitude = lon)$azimuth

    df_case$sunAzimuth_f <- as.character(cut(ifelse(df_case$sunAzimuth>=(360-22.5),df_case$sunAzimuth-360,df_case$sunAzimuth),seq(-22.5,360-22.5,45),
                       labels = c("N","NE","E","SE","S","SO","O","NO")))
    df_case$sunAzimuth_f <- as.factor(ifelse(df_case$sunElevation<=0,"NA",df_case$sunAzimuth_f))
    df_case$sunElevation_f <- cut(df_case$sunElevation,c(-180,0,10,20,30,40,50,60,70,80,90,180),
            labels = c("NA","5","15","25","35","45","55","65","75","85","95"))
    df_case$sunPosition <- as.factor(ifelse(as.character(df_case$sunElevation_f)=="NA","NA",
            paste0("_",as.character(df_case$sunAzimuth_f),"_",as.character(df_case$sunElevation_f))))

    df_case$sunElevation <- ifelse(df_case$sunElevation<0,0,df_case$sunElevation)#ifelse(df_case$sun_elevation<0,0,ifelse(df_case$sun_elevation<30,1,2))
    df_case$sunAzimuth <- ifelse(df_case$sunElevation==0,0,df_case$sunAzimuth)


    # Filter by complete cases and seasonality, if needed
    df_case<-df_case[complete.cases(df_case),]
    if (!is.null(str_val)) df_case<- df_case[df_case$s==str_val,]
    # Time column to seconds
    df_case$hours <- (as.numeric(strftime(df_case$time,"%s"))-as.numeric(strftime(df_case$time[1],"%s")))/3600

    if(exists("model", where=model)){
        all_columns = colnames(model$model$training_dataset)
        this_columns = colnames(df_case)
        missing_columns = all_columns[!all_columns %in% this_columns]
        print(missing_columns)
        for( c in missing_columns){
            if (class(model$model$training_dataset[,c])=='factor'){
                df_case[,c] <- as.factor("NA")
                levels(df_case[,c]) <- levels(model$model$training_dataset[,c])
            } else{
                df_case[,c] <- NA
            }

            print(c)
        }
        all_columns = colnames(model$model$training_dataset)
        this_columns = colnames(df_case)
        if(length(all_columns[!all_columns %in% this_columns]) == 0){
            print("solved!")
        }else{
            print("error")
            print(all_columns[!all_columns %in% this_columns])
        }

    }
    return(df_case)
}

lp_vector<- function(x, a1) {
    ## Make a 1'st order low pass filter as (5.3) p.46 in the HAN report.
    y <- numeric(length(x))
    ## First value in x is the init value
    y[1] <- x[1]
    ##
    for (i in 2:length(x)) {
        if (is.na(y[i - 1])) {
            y[i] <- x[i]
        } else {
            y[i] <- a1 * y[i - 1] + (1 - a1) * x[i]
        }
    }
    ## Return (afterwards the init value y[1], must be handled)
    return(y)
}

tune_inputs <- function(df, value_column, n, m, alpha_t, alpha_g, alpha_w, tbal, p){
    # Low pass filtering in the weather data.
    df$temperature <- lp_vector(df$temperature, a1 = alpha_t)
    df$windSpeed <- lp_vector(df$windSpeed, a1=alpha_w)
    df$GHI <- lp_vector(df$GHI, a1=alpha_g)

    # Calculate the weather and lagged consumption data depending the usage level.
    df <- add_all_raw_features_occ(df=df, percentual_threshold=p, value_column=value_column)

    # Calculate the delta T between the balance temperature and the tweaked outdoor temperature
    df <- add_cooling_heating_deltaT_features_by_occupancy(df, tbal)

    return(df)
}

add_lagged_features_occ <- function(df,value_column){
    lags_cols <- colnames(df)[grepl(paste0("^",value_column,"_lag"),colnames(df))]
    lags_cols <- lags_cols[!grepl("_s",lags_cols)]
    lags_cols <- as.numeric(gsub(paste0(value_column,"_lag"),"",lags_cols))
    for (i in lags_cols){
        df[,paste0(value_column,"_occ_lag",i)]<-ifelse(df$occupancy_tf==1,df[,paste0(value_column,"_lag",i)],log(0.01))
        for (l in unique(df$s)){
            df[,paste0(value_column,"_occ_lag",i,"_s",l)]<-ifelse(df$s==l,df[,paste0(value_column,"_occ_lag",i)],log(0.01))
        }
    }
    return(df)
}

add_weather_features_occ <- function(df){
    df$temperature_occ <- df$temperature * df$occupancy_tf
    df$windSpeed_occ <- df$windSpeed * df$occupancy_tf
    df$GHI_occ <- df$GHI * df$occupancy_tf
    return(df)
}

add_all_raw_features_occ <- function(df, percentual_threshold, value_column){
    df$occupancy_tf <- ifelse(df$occupancy<percentual_threshold,0,1)
    for (l in unique(df$s)){
    df[,paste0("occupancy_tf_s",l)] <- as.factor(ifelse(df$s==l & df$occupancy_tf==1, "TRUE", "FALSE"))
    }
    df <- add_lagged_features_occ(df,value_column)
    df <- add_weather_features_occ(df)
    return(df)
}

add_cooling_heating_deltaT_features_by_occupancy <- function(df, tbal){
    df$temperature_h_occ <- ifelse(df$temperature >= tbal, 0, tbal - df$temperature)
    df$temperature_occ <- ifelse(df$temperature <= (tbal-2),
                               (df$temperature - tbal) * df$occupancy_tf,
                               ifelse( df$temperature >= (tbal+2),
                                       (df$temperature - tbal) * df$occupancy_tf,
                                       0 )
    )
    df$temperature_c_occ <-
    ifelse(df$temperature <= tbal, 0, df$temperature - tbal) *
    df$occupancy_tf
    return(df)
}

detect_days_with_no_usage <- function(df, value_column, time_column, tz="UTC"){
    Sys.setenv(TZ=tz)
    df <- df[df[,value_column]<=quantile(df[,value_column],0.7,na.rm=T),]
    # Consider the Probability Density Function of the 90% of the smaller measures.
    d <- density(df[,value_column],na.rm=T)
    # calculate the local extremes
    tp<-turnpoints(ts(d$y))
    # defines the local maximums and minimums
    importance <- d$y[tp$tppos]
    cons <- d$x[tp$tppos]
    shifted_importance <- c(0,shift(importance,1)[is.finite(shift(importance,1))])
    min_max <- ifelse(importance-shifted_importance>0,"max","min")
    cons_max <- c(NA,cons[min_max=="max"])
    importance_max <- c(NA,importance[min_max=="max"])
    # If no more than 2 local extremes are detected in the PDF, assume a consumption limit of 0.
    cons_limit <- 0.0
    if (length(importance)>=3){
        # Initially assume the first minimum as the consumption limit if the first maximum is lower or equal to the
        # percentile 20 and its importance in the PDF is higher than the 20% of the maximum importance detected.
        if (cons[1]<=quantile(df[,value_column],0.2,na.rm=T) & importance[1] > 0.2*max(importance)) cons_limit <- cons[2]
        # Select as the consumption limit, the minimum before the first important, or relevant, mode of the distribution.
        # Select the first important mode considering that:
        # - It must has a difference higher than a 200% over its previous maximum
        # - It cannot be the first maximum (then, no first important mode is detected, thus any day is deleted from the dataset),
        # - Its consumption must be higher than the percentile 10 of the daily consumption distribution.
        first_important_mode <- mapply(function(i){
            differ <- ((importance_max[i+1]-importance_max[i])/importance_max[i])
            ((differ*100 > 200  & is.finite(differ)) & cons_max[i+1]>quantile(df[,value_column],0.1,na.rm=T))
        },1:(sum(min_max=="max")))
        if(any(first_important_mode)==T) cons_limit <- cons[which(importance == importance_max[which.max(first_important_mode)+1])-1]
        # if the first important mode has a daily consumption of less than 0.5kWh (15 kWh per month) then select as
        # cons_limit the higher minimum closest to this first important mode
        if (cons_max[which.max(first_important_mode)+1][1]<0.5) cons_limit <- cons[which(importance == importance_max[which.max(first_important_mode)+1])+1]
    }
    if (length(cons_limit)==0){ cons_limit <- 0 }
    if (length(cons)==1){
        if(cons < 0.5) cons_limit <- cons
    }
    days_detected <- as.Date(df[df[,value_column] <= cons_limit & is.finite(df[,value_column]),time_column], tz=tz)
    # return
    if(length(days_detected)==0){
        return(NULL)
    }else{
        return(unique(days_detected))
    }
}

delete_days_with_no_usage <- function(df, days_to_delete, value_column, time_column, tz){
    Sys.setenv(TZ=tz)
    if(!is.null(days_to_delete)){
        dates <- as.Date(df[,time_column], tz=tz)
        df[(dates %in% days_to_delete),value_column] <- NA
    }
    return(df)
}

### Model training functions ----

train_gam <-function(model, type, df, value_column, by_s, n_max=0, m_max=0){
    ini_time <- Sys.time()
    training_period <- sample(unique(df$date),length(unique(df$date))*80/100,replace = F)
    validation_period <- unique(df$date)[!(unique(df$date) %in% training_period)]
    df_case_train = df[df$date %in% training_period,]
    df_case_val = df[df$date %in% validation_period,]
    # Feature selection of the model
    if(by_s==F){
        features <- select_features_by_str_change(type, df_case_train,value_column,n_max,m_max,by_s)
    } else {
        features <- select_features(type, df_case_train,value_column,n_max,m_max,by_s)
    }

    # Optimize the n, m, low pass filtering alpha for temperature feature and balance temperature
    if (n_max>0 || m_max>0){
        min_per_feature = c(0,0,0,0,0,16,1)
        max_per_feature = c(n_max,m_max,0.5,0.5,0.5,24,30)
        nclasses_per_feature = c(n_max,m_max,15,15,15,15,15)
        class_per_feature = c("int","int","float","float","float","float","float")
        names_per_feature = c("n","m","alpha_t","alpha_w","alpha_g","tbal","p")
        GAscenario <- suppressMessages(
            ga(
                type = "binary",
                fitness = optimize_model,
                nBits = sum(mapply(function(x) { nchar(toBin(x)) }, nclasses_per_feature)),
                min_per_feature = min_per_feature,
                max_per_feature = max_per_feature,
                nclasses_per_feature = nclasses_per_feature,
                class_per_feature = class_per_feature,
                names_per_feature = names_per_feature,
                popSize = 24, maxiter = 3, monitor = gaMonitor2,
                parallel = 8, elitism=1, pmutation=0.1, keepBest = T,
                df_case_train=df_case_train, df_case_val=df_case_val,
                value_column=value_column, formula_features=features$formula, by_s=by_s
            )
        )
        params <- decodeValueFromBin(GAscenario@solution[1,], class_per_feature, nclasses_per_feature, min_per_feature, max_per_feature)
        names(params) <- names_per_feature
        print(params)
    }

    # Detect the features which are no longer needed in the final model
    mod<- gam_model(n = params["n"], m = params["m"], alpha_t = params["alpha_t"], alpha_w = params["alpha_w"],
                  alpha_g = params["alpha_g"], tbal = params["tbal"], p = params["p"],
                  df_case_train, value_column, features$formula,by_s)

    # Validation dataset changes
    df_case_val <- tune_inputs(n = mod[["n"]], m = mod[["m"]], alpha_t = mod[["alpha_t"]], alpha_w = mod[["alpha_w"]],
                             alpha_g = mod[["alpha_g"]], tbal = mod[["tbal"]], p = mod[["p"]],
                             df = df_case_val, value_column = value_column)

    # Prediction (over validation dataset) with the final model
    prediction <- predict(mod,newdata=df_case_val)
    mod <- modifyList(mod,
                    calculate_fitness(mod,
                                      df = data.frame(exp(prediction), exp(df_case_val[,value_column])),
                                      value_column = value_column, for_optimization = F)
    )

    # Computational time
    elapsed_time <- Sys.time() - ini_time
    mod$elapsed_time <- as.numeric(elapsed_time,units="secs")
    model$model <- mod
    return(model)
}




select_features <- function(type, df_case_train, value_column, n, m,by_s){

    if (type=="electricity") {
        formula_ini = as.formula(paste0(value_column," ~ 1 + s(dayhour,bs='cc') + s(dayweek,bs='cc')"))
    } else if (type=="gas") {
        formula_ini = as.formula(paste0(value_column," ~ 1 + as.factor(as.logical(occupancy_tf))"))
    }

    occupancy = "s(occupancy,bs='cs',k=2)"
    temperature = "ti(temperature_occ,hours,bs='cs',k=2)"
    temperature_h = "s(temperature_h_occ,bs='cs',k=2)"
    temperature_c = "s(temperature_c_occ,bs='cs',k=2)"
    sun_elevation = "ti(sunAzimuth,sunElevation,bs='cs',k=4)"
    GHI = "s(GHI,bs='cs',k=3)"
    GHIelev = "ti(sunElevation,GHI,bs='cs',k=4)"
    GHIazi = "ti(sunAzimuth,GHI,bs='cc',k=4)"
    GHIpos = "s(GHI,by=sunPosition,bs='cs',k=2)"
    wind_temp = "ti(windSpeed_occ,temperature,bs='cs',k=4)"
    wind_spdir = "ti(windSpeed_occ,windDirection,bs='cc',k=4)"
    windpos = "s(windSpeed_occ,by=windDirection_f,bs='cs',k=3)"
    wind = "s(windSpeed_occ,bs='cs',k=3)"

    list_features <- list(occupancy, temperature, GHIpos, windpos)
    formula <- update.formula(formula_ini,paste0(". ~ . + ",do.call(paste,list(list_features,collapse=" + "))))
    mod <- gam_model(n=0,m=0,alpha_t = 0,alpha_w = 0,alpha_g = 0,tbal=18, p = 0,
                   df_case_train = df_case_train,value_column = value_column,features_formula = formula,by_s = by_s)
    summ_mod<-summary(mod)
    coeffs <- mapply(function(x){sub(":.*","",x)},rownames(summ_mod$s.table))
    coeffs_pvalue <- aggregate(data.frame(pvalue=summ_mod$s.table[,'p-value']),by=list("coeffs"=coeffs),mean)
    rownames(coeffs_pvalue) <- coeffs_pvalue$coeffs
    coeffs_pvalue <- coeffs_pvalue[coeffs[!duplicated(coeffs)],]
    list_features <- list_features[coeffs_pvalue$pvalue<=0.05] # only get those features with p-value lower than 0.01
    best_formula <-  update.formula(formula_ini,paste0(". ~ . + ",do.call(paste,list(list_features,collapse=" + "))))

    return(list(formula=best_formula,list=list_features,formula_ini=formula_ini))

}

select_features_by_str_change <- function(type, df_case_train, value_column,n,m,by_s){

    if (type=="electricity") {
        if (length(unique(df_case_train$s))==1){
            formula_ini = as.formula(paste0(value_column," ~ 1 + s(dayhour,bs='cs',k=6) + s(dayweek,k=3)"))
        } else {
            formula_ini = as.formula(paste0(value_column," ~ 0 + s + s(dayhour,bs='cs',by=s,k=6) + s(dayweek,bs='cs',by=s,k=3)"))#", paste0("dayhour_f_s",unique(df_case_train$s),collapse="+")," + ",
                                          #paste0("dayweek_f_s",unique(df_case_train$s),collapse="+")))
        }
    } else if (type=="gas") {
        if (length(unique(df_case_train$s))==1){
            formula_ini = as.formula(paste0(value_column," ~ 1 + dayhour_f"))#as.factor(as.logical(occupancy_tf))"))
        } else {
            formula_ini = as.formula(paste0(value_column," ~ 0 + s + ", #paste0("occupancy_tf_s",unique(df_case_train$s),collapse="+")))#," + ",
                                          paste0("dayhour_f_s",unique(df_case_train$s),collapse="+")))#," + ",
          #paste0("dayweek_f_s",unique(df_case_train$s),collapse="+")))
        }
    }

    occupancy = "s(occupancy,bs='cs',k=4)"
    temperature = "s(temperature_occ,bs='cs',k=2)"
    changes_in_schedule = "s(dayhour, bs='cc',k=24)"
    temperature_h = "s(temperature_h_occ,bs='cs',k=2)"
    temperature_c = "s(temperature_c_occ,bs='cs',k=2)"
    sun_elevation = "ti(sunAzimuth,sunElevation,bs='cs',k=4)"
    GHI = "s(GHI_occ,bs='cs',k=3)"
    GHIelev = "ti(sunElevation,GHI_occ,bs='cs',k=4)"
    GHIazi = "ti(sunAzimuth,GHI_occ,bs='cc',k=4)"
    GHIpos = "s(GHI,by=sunPosition,bs='cs',k=2)"
    wind_temp = "ti(windSpeed_occ,temperature,bs='cs',k=4)"
    wind_spdir = "ti(windSpeed_occ,windDirection,bs='cc',k=4)"
    windpos = "s(windSpeed_occ,by=windDirection_f,bs='cs',k=3)"
    wind = "s(windSpeed,bs='cs',k=3)"
    if (type=="gas"){
        list_features <- list(temperature, GHIpos, windpos)
    } else if(type=="electricity"){
        list_features <- list(temperature, GHIpos, windpos)
    }
    formula <- update.formula(formula_ini,paste0(". ~ . + ",do.call(paste,list(list_features,collapse=" + "))))
    mod <- gam_model(n=1,m=0,alpha_t = 0.05,alpha_w = 0.05,alpha_g = 0.05,tbal=20, p = 15,
                   df_case_train = df_case_train,value_column = value_column,features_formula = formula,by_s = by_s)
    summ_mod<-summary.gam(mod)
    coeffs <- mapply(function(x){sub(":.*","",x)},rownames(summ_mod$s.table))
    coeffs_pvalue <- aggregate(data.frame(pvalue=summ_mod$s.table[,'p-value']),by=list("coeffs"=coeffs),mean)
    rownames(coeffs_pvalue) <- coeffs_pvalue$coeffs
    coeffs_pvalue <- coeffs_pvalue[coeffs[!duplicated(coeffs)],]
    list_features <- list_features[which(coeffs_pvalue$pvalue<=0.05)] # only get those features with p-value lower than 0.05
    best_formula <-  update.formula(formula_ini,paste0(". ~ . + ",do.call(paste,list(list_features,collapse=" + "))))

    return(list(formula=best_formula,list=list_features,formula_ini=formula_ini))
}


train_linear <-function(model, type, df, value_column, by_s, n_max=0, m_max=0){
    ini_time <- Sys.time()
    # Feature selection of the model
    training_period <- sample(unique(df$date),length(unique(df$date))*80/100,replace = F)
    validation_period <- unique(df$date)[!(unique(df$date) %in% training_period)]
    df_case_train = df[df$date %in% training_period,]
    df_case_val = df[df$date %in% validation_period,]
    if(by_s==F){
        features <- select_linear_features_by_str_change(type,df_case_train,value_column,n_max,m_max,by_s)
    } else {
        features <- select_linear_features(type,df_case_train,value_column,n_max,m_max,by_s)
    }

    # Optimize the n, m, low pass filtering alpha for temperature feature and balance temperature
    if (n_max>0 || m_max>0){
        min_per_feature = c(0,0,0,0,0,16,1)
        max_per_feature = c(n_max,m_max,0.5,0.5,0.5,24,30)
        nclasses_per_feature = c(n_max,m_max,15,15,15,15,15)
        class_per_feature = c("int","int","float","float","float","float","float")
        names_per_feature = c("n","m","alpha_t","alpha_w","alpha_g","tbal","p")
        GAscenario <- suppressMessages(
            ga(
                type = "binary",
                fitness = optimize_linear_model,
                nBits = sum(mapply(function(x) { nchar(toBin(x)) }, nclasses_per_feature)),
                min_per_feature = min_per_feature,
                max_per_feature = max_per_feature,
                nclasses_per_feature = nclasses_per_feature,
                class_per_feature = class_per_feature,
                names_per_feature = names_per_feature,
                popSize = 10, maxiter = 4, monitor = gaMonitor2,
                parallel = 1, elitism=1, pmutation=0.1, keepBest = T,
                df_case_train=df_case_train, df_case_val=df_case_val,
                value_column=value_column, formula_features=features$formula, by_s=by_s
            )
        )
        params <- decodeValueFromBin(GAscenario@solution[1,], class_per_feature, nclasses_per_feature, min_per_feature, max_per_feature)
        names(params) <- names_per_feature
        print(params)
    }
    # Detect the features which are no longer needed in the final model
    mod<- linear_model(n = params["n"], m = params["m"], alpha_t = params["alpha_t"], alpha_w = params["alpha_w"],
                     alpha_g = params["alpha_g"], tbal = params["tbal"], p = params["p"],
                     df_case_train, value_column, features$formula,by_s)

    # Validation dataset changes
    df_case_val <- tune_inputs(n = mod[["n"]], m = mod[["m"]], alpha_t = mod[["alpha_t"]], alpha_w = mod[["alpha_w"]],
                             alpha_g = mod[["alpha_g"]], tbal = mod[["tbal"]], p = mod[["p"]],
                             df = df_case_val, value_column=value_column)

    # Prediction (over validation dataset) with the final model
    prediction <- predict(mod,newdata=df_case_val)
    mod <- modifyList(mod,
                    calculate_fitness(mod,
                                      df = data.frame(exp(prediction), exp(df_case_val[,value_column])),
                                      value_column = value_column, for_optimization = F)
    )

    # Computational time
    elapsed_time <- Sys.time() - ini_time
    mod$elapsed_time <- as.numeric(elapsed_time,units="secs")
    model$model <- mod
    return(model)
}

clean_linear <- function(model){
    model$df <- NULL
    model$model$model <- NULL
    model$model$residuals <- NULL
    model$model$fitted.values <- NULL
    model$model$effects <- NULL
    model$model$df.residual <- NULL
    model$model$assign <- NULL
    model$model$xlevels <- NULL
    model$model$training_dataset<-head(model$model$training_dataset)
    return(model)
}

select_linear_features <- function(type, df_case_train, value_column, n, m,by_s){

    if (type=="electricity") {
        formula_ini = as.formula(paste0(value_column," ~ 1 + dayhour_f + dayweek_f"))
    } else if (type=="gas") {
        formula_ini = as.formula(paste0(value_column," ~ 1 + as.factor(as.logical(occupancy_tf))"))
    }

    occupancy = "occupancy"
    temperature = "bs(temperature_occ, knots=2)"
    temperatureh = "temperature_h_occ"
    temperaturec = "temperature_c_occ"
    GHI = "GHI_occ/sunPosition"
    wind = "windSpeed_occ/windDirection_f"

    list_features <- list(occupancy, temperatureh, temperaturec, GHI, wind)
    formula <- update.formula(formula_ini,paste0(". ~ . + ",do.call(paste,list(list_features,collapse=" + "))))
    mod <- linear_model(n=0,m=0,alpha_t = 0,alpha_w = 0,alpha_g = 0,tbal=18, p = 0,
                      df_case_train = df_case_train,value_column = value_column,features_formula = formula,by_s = by_s)
    summ_mod<-summary(mod)
    coeffs_pvalue <- data.frame(coeffs = rownames(summ_mod$coefficients), pvalue=summ_mod$coefficients[,'Pr(>|t|)'])
    coeffs_pvalue <- coeffs_pvalue[!grepl(pattern = "dayweek|dayhour",coeffs_pvalue$coeffs),]
    list_features <- list_features[mapply(function(x){ any(coeffs_pvalue$pvalue[grepl(x,coeffs_pvalue$coeffs,fixed=T)]<=0.05) }, unlist(list_features))] # only get those features with p-value lower than 0.05
    best_formula <-  update.formula(formula_ini,paste0(". ~ . + ",do.call(paste,list(list_features,collapse=" + "))))

    return(list(formula=best_formula,list=list_features,formula_ini=formula_ini))
}

select_linear_features_by_str_change <- function(type, df_case_train, value_column,n,m,by_s){

    if (type=="electricity") {
        if (length(unique(df_case_train$s))==1){
            formula_ini = as.formula(paste0(value_column," ~ 1 + dayhour_f"))
            #formula_ini = as.formula(paste0(value_column," ~ 1"))
        } else {
            formula_ini = as.formula(paste0(value_column," ~ 0 + s + dayhour_f:s"))#, paste0("dayhour_f_s",unique(df_case_train$s),collapse="+")," + ",
            #                                 paste0("dayweek_f_s",unique(df_case_train$s),collapse="+")))
            #formula_ini = as.formula(paste0(value_column," ~ 0 + s"))
        }
    } else if (type=="gas") {
        if (length(unique(df_case_train$s))==1){
            formula_ini = as.formula(paste0(value_column," ~ 1 + dayhour_f"))#as.factor(as.logical(occupancy_tf))"))
        } else {
            formula_ini = as.formula(paste0(value_column," ~ 0 + s + ", #paste0("occupancy_tf_s",unique(df_case_train$s),collapse="+")))#," + ",
                                          paste0("dayhour_f_s",unique(df_case_train$s),collapse="+")))#," + ",
          #paste0("dayweek_f_s",unique(df_case_train$s),collapse="+")))
        }
    }

    occupancy = "occupancy"
    temperature = "bs(temperature_occ, knots=2)"
    temperaturec = "temperature_c_occ"
    temperatureh = "temperature_h_occ"
    GHI = if(length(unique(df_case_train$sunPosition))>1){"GHI_occ/sunPosition"}else{"GHI_occ"}
    wind = if(length(unique(df_case_train$windDirection_f))>1){"windSpeed_occ/windDirection_f"}else{"windSpeed_occ"}

    if (type=="gas"){
        list_features <- list(occupancy,temperaturec, temperatureh, GHI, wind)
    } else if(type=="electricity"){
        list_features <- list(occupancy,temperaturec, temperatureh, GHI, wind)
    }
    formula <- update.formula(formula_ini,paste0(". ~ . + ",do.call(paste,list(list_features,collapse=" + "))))
    mod <- linear_model(n=1,m=0,alpha_t = 0.05,alpha_w = 0.05,alpha_g = 0.05,tbal=20, p = 15,
                      df_case_train = df_case_train,value_column = value_column,features_formula = formula,by_s = by_s)
    summ_mod<-summary(mod)
    coeffs_pvalue <- data.frame(coeffs = rownames(summ_mod$coefficients), pvalue=summ_mod$coefficients[,'Pr(>|t|)'])
    coeffs_pvalue <- coeffs_pvalue[!grepl(pattern = "dayweek|dayhour",coeffs_pvalue$coeffs),]
    list_features <- list_features[mapply(function(x){ any(coeffs_pvalue$pvalue[grepl(x,coeffs_pvalue$coeffs,fixed=T)]<=0.05) }, unlist(list_features))] # only get those features with p-value lower than 0.05
    best_formula <-  update.formula(formula_ini,paste0(". ~ . + ",do.call(paste,list(list_features,collapse=" + "))))

    return(list(formula=best_formula,list=list_features,formula_ini=formula_ini))

}


gam_model <- function(n, m, alpha_t, alpha_g, alpha_w, tbal, p, df_case_train, value_column, features_formula=.~.,by_s){

    # Define the formula without lags
    formula <- as.formula(paste0(value_column," ~ ."))
    formula <- update.formula(formula, features_formula)
    # Delete the occupancy features with no more than 1 contrast
    formula_indep_features <- strsplit(as.character(formula)[3]," + ",fixed=T)[[1]]
    formula_indep_features <- formula_indep_features[mapply(function(i){
        if(grepl("occupancy_tf_s",i)){
            length(unique(df_case_train[,i]))>1
        }else{
            T
        }
    },formula_indep_features)]

    formula <- as.formula(paste0(value_column," ~ ",paste(formula_indep_features,collapse=" + ")))

    df_case_train <- tune_inputs(df_case_train, value_column, n, m, alpha_t, alpha_g, alpha_w, tbal, p)

    if( sum(df_case_train$temperature_h_occ>0) <= nrow(df_case_train)*0.2 ){
        formula <- as.formula(paste0(strsplit(as.character(formula), "[+]")[[2]],"~",
                                 paste(strsplit(as.character(formula), "[+]")[[3]][!grepl("temperature_h_occ",strsplit(as.character(formula), "[+]")[[3]])],collapse="+")))
    }
    if( sum(df_case_train$temperature_c_occ>0) <= nrow(df_case_train)*0.2 ){
        formula <- as.formula(paste0(strsplit(as.character(formula), "[+]")[[2]],"~",
                                 paste(strsplit(as.character(formula), "[+]")[[3]][!grepl("temperature_c_occ",strsplit(as.character(formula), "[+]")[[3]])],collapse="+")))
    }

    # Consider the inertia (lag) features into the model
    inertia_features <- NULL
    if(length(unique(df_case_train$s))==1){ by_s=T }
    if(by_s==F){
        if (n>0){ inertia_features <- paste0(value_column,"_lag",1:n,":s") }
        if (m>0){ inertia_features <- c(inertia_features, paste0(value_column,"_lag",(1:m)*24,":s")) }
        #if (n>0){ inertia_features <- c(inertia_features, paste0(value_column,"_occ_lag",1:n,":s")) }
        #if (m>0){ inertia_features <- c(inertia_features, paste0(value_column,"_occ_lag",(1:m)*24,":s")) }
    } else {
        if (n>0){ inertia_features <- paste0(value_column,"_lag",1:n) }
        if (m>0){ inertia_features <- c(inertia_features, paste0(value_column,"_lag",(1:m)*24)) }
        #if (n>0){ inertia_features <- c(inertia_features, paste0(value_column,"_occ_lag",1:n)) }
        #if (m>0){ inertia_features <- c(inertia_features, paste0(value_column,"_occ_lag",(1:m)*24)) }
    }
    if (!is.null(inertia_features)){ formula = update.formula(formula,as.formula(paste0(value_column," ~ .+",paste(inertia_features,collapse="+")))) }

    # Estimate the model
    df_case_train <- df_case_train[complete.cases(df_case_train),]
    mod<-gam(formula = formula,data=df_case_train)
    cat(".")
    #write.table(df_case_train,"tes.csv",sep=";",eol="\r")

    # Add the hyperparameters and feature engineering coefficients to the model object
    mod$alpha_t <- alpha_t
    mod$alpha_w <- alpha_w
    mod$alpha_g <- alpha_g
    mod$p <- p
    mod$value_column <- value_column
    mod$tbal <- tbal
    mod$n <- n
    mod$m <- m
    mod$training_dataset <- df_case_train

    return(mod)
}

linear_model <- function(n, m, alpha_t, alpha_g, alpha_w, tbal, p, df_case_train, value_column, features_formula=.~.,by_s){

    # Define the formula without lags
    formula <- as.formula(paste0(value_column," ~ ."))
    formula <- update.formula(formula, features_formula)
    # Delete the occupancy features with no more than 1 contrast
    formula_indep_features <- strsplit(as.character(formula)[3]," + ",fixed=T)[[1]]
    formula_indep_features <- formula_indep_features[mapply(function(i){
        if(grepl("occupancy_tf_s",i)){
            length(unique(df_case_train[,i]))>1
        }else{
            T
        }
    },formula_indep_features)]
    formula <- as.formula(paste0(value_column," ~ ",paste(formula_indep_features,collapse=" + ")))

    df_case_train <- tune_inputs(df_case_train, value_column, n, m, alpha_t, alpha_g, alpha_w, tbal, p)

    if( sum(df_case_train$temperature_h_occ>0) <= nrow(df_case_train)*0.2 ){
        formula <- as.formula(paste0(strsplit(as.character(formula), "[+]")[[2]],"~",
                                 paste(strsplit(as.character(formula), "[+]")[[3]][!grepl("temperature_h_occ",strsplit(as.character(formula), "[+]")[[3]])],collapse="+")))
    }
    if( sum(df_case_train$temperature_c_occ>0) <= nrow(df_case_train)*0.2 ){
        formula <- as.formula(paste0(strsplit(as.character(formula), "[+]")[[2]],"~",
                                 paste(strsplit(as.character(formula), "[+]")[[3]][!grepl("temperature_c_occ",strsplit(as.character(formula), "[+]")[[3]])],collapse="+")))
    }

    # Consider the inertia (lag) features into the model
    inertia_features <- NULL
    if(length(unique(df_case_train$s))==1){ by_s=T }
    if(by_s==F){
        if (n>0){ inertia_features <- paste0(value_column,"_lag",1:n,":s") }
        if (m>0){ inertia_features <- c(inertia_features, paste0(value_column,"_lag",(1:m)*24,":s")) }
        #if (n>0){ inertia_features <- c(inertia_features, paste0(value_column,"_occ_lag",1:n,":s")) }
        #if (m>0){ inertia_features <- c(inertia_features, paste0(value_column,"_occ_lag",(1:m)*24,":s")) }
    } else {
        if (n>0){ inertia_features <- paste0(value_column,"_lag",1:n) }
        if (m>0){ inertia_features <- c(inertia_features, paste0(value_column,"_lag",(1:m)*24)) }
        #if (n>0){ inertia_features <- c(inertia_features, paste0(value_column,"_occ_lag",1:n)) }
        #if (m>0){ inertia_features <- c(inertia_features, paste0(value_column,"_occ_lag",(1:m)*24)) }
    }
    if (!is.null(inertia_features)){ formula = update.formula(formula,as.formula(paste0(value_column," ~ .+",paste(inertia_features,collapse="+")))) }

    # Estimate the model
    df_case_train <- df_case_train[complete.cases(df_case_train),]
    mod<-lm(formula = formula,data=df_case_train)
    cat(".")
    #write.table(df_case_train,"tes.csv",sep=";",eol="\r")

    # Add the hyperparameters and feature engineering coefficients to the model object
    mod$alpha_t <- alpha_t
    mod$alpha_w <- alpha_w
    mod$alpha_g <- alpha_g
    mod$p <- p
    mod$value_column <- value_column
    mod$tbal <- tbal
    mod$n <- n
    mod$m <- m
    mod$training_dataset <- df_case_train

    return(mod)
}



### Auxiliar functions for model optimization ----


improved.whitenoise.test <- function(x){
    x <- x[x >quantile(x,0.01,na.rm=T) & x < quantile(x,0.99,na.rm=T)]
    n <- length(x)
    gam <- acf(x, type = "covariance", plot=FALSE)
    gam0 <- gam$acf[1]
    ILAM <- spec.pgram(x, taper = 0, fast = FALSE, plot=FALSE)
    ILAM <- ILAM$spec
    T <- length(ILAM)
    P2 <- (T^(-1)) * (sum(ILAM^2))
    MN <- (P2/gam0^2) - 1
    tMN <- sqrt(T) * (MN - 1)
    pval <- pnorm(tMN, mean = 0, sd = 2, lower.tail = FALSE)
    test <- pval * 2
    if (test > 1)
        test <- 2 - test
    return(test)
}

analyze_correlation <- function(residuals){
    x <- acf(residuals, plot=F)
    n_corr = 0
    conf = qnorm((1+0.95)/2)/sqrt(x$n.used)
    for(res in x$acf){
        if(abs(res) > conf){
            n_corr= n_cor + 1
        }
    }
    return (n_corr)
}

calculate_fitness <- function(mod, df, value_column, for_optimization=T){
    #wnt <- hwwn.test(mod$residuals[1:(2^floor(log(length(mod$residuals),base = 2)))])$p.value
    #wnt <- ifelse(wnt<0.05,10,1) # if wnt<0.05, the H0 of white noise is rejected.
    pacf_training <- pacf(exp(mod$model[,value_column]) - exp(mod$fitted.values),plot = F,lag.max = max(mod$m*24,mod$n,23))$acf
    error_training <- rmserr(exp(mod$model[,value_column]), exp(mod$fitted.values))
    nrmse_training <- error_training$rmse / mean(exp(mod$model[,value_column]),na.rm=T)
    error_validation <- rmserr(df[,1], df[,2])
    nrmse_validation <- error_validation$rmse / mean(df[,2],na.rm=T)
    #score <- (sum(abs(pacf_training)>(1.96*1/sqrt(nrow(mod$training_dataset))))+0.1) * nrmse_training * nrmse_validation # * error_training$mape * error_validation$mape
    score <- cor(df[,1],df[,2])^2

    if(for_optimization==T){
        return(score)
    } else {
        return(list(
            "pacf_t" = pacf_training,
            "nrmse_t" = nrmse_training,
            "nrmse_v" = nrmse_validation,
            "mape_t" = error_training$mape,
            "mape_v" = error_validation$mape))
    }
}


decodeValueFromBin <- function(binary_representation, class_per_feature, nclasses_per_feature,
                               min_per_feature, max_per_feature){
    bitOrders <- mapply(function(x) { nchar(toBin(x)) }, nclasses_per_feature)
    binary_representation <- split(binary_representation, rep.int(seq.int(bitOrders), times = bitOrders))
    orders <- sapply(binary_representation, function(x) { binary2decimal(gray2binary(x)) })
    orders <- mapply(function(x){min(orders[x],nclasses_per_feature[x])},1:length(orders))
    orders <- mapply(
        function(x){
            switch(class_per_feature[x],
                 "int"= floor(seq(min_per_feature[x],max_per_feature[x],
                                  by=(max_per_feature[x]-min_per_feature[x])/(nclasses_per_feature[x]))[orders[x]+1]),
                 "float"= seq(min_per_feature[x],max_per_feature[x],
                              by=(max_per_feature[x]-min_per_feature[x])/(nclasses_per_feature[x]))[orders[x]+1]
            )
        }
        ,1:length(orders)
    )
    return(unname(orders))
}

toBin<-function(x){ as.integer(paste(rev( as.integer(intToBits(x))),collapse="")) }

gaMonitor2 <- function (object, digits = getOption("digits"), ...){
    fitness <- na.exclude(object@fitness)
    cat(paste("GA | Iter =", object@iter, " | Mean =", format(mean(fitness),
                                                            digits = digits), " | Best =", format(max(fitness),
                                                                                                  digits = digits), "\n"))
    flush.console()
}


### Model optimization functions ----


optimize_model <- function(params, nclasses_per_feature, min_per_feature, max_per_feature, class_per_feature, names_per_feature,
                           df_case_train, df_case_val, value_column, formula_features, by_s){

    # Train the model considering the params argument
    params <- decodeValueFromBin(params, class_per_feature, nclasses_per_feature, min_per_feature, max_per_feature)
    # params <- c()
    # params$n <- 12
    # params$m <- 0
    # params$alpha_t <- 0.6
    # params$alpha_w <- 0.6
    # params$alpha_g <- 0.6
    # params$tbal <- 16
    # params$p <- 1
    # params <- unlist(params)
    names(params) <- names_per_feature
    mod <- gam_model(n = params["n"], m = params["m"], alpha_t = params["alpha_t"], alpha_w = params["alpha_w"],
                   alpha_g = params["alpha_g"], tbal = params["tbal"], p = params["p"],
                   df_case_train = df_case_train, value_column = value_column,
                   features_formula = formula_features, by_s = by_s)

    mod_lst = list("model"=mod)
    if(by_s == T){
        prediction <- predict_mod_by_s(mod_lst, head(df_case_val, 24*3))$pred
    }else{
        prediction <- predict_mod_all(mod_lst, head(df_case_val,24*3))$pred
    }
    # # Validation dataset changes
    # df_case_val <- tune_inputs(n = params["n"], m = params["m"], alpha_t = params["alpha_t"], alpha_w = params["alpha_w"],
    #                          alpha_g = params["alpha_g"], tbal = params["tbal"], p = params["p"],
    #                          df = df_case_val, value_column = value_column)
    #
    # # Consumption prediction
    # prediction <- predict(mod,newdata=df_case_val)
    #
    # # Calculate the score
    score <- calculate_fitness(mod, data.frame(exp(prediction), exp(head(df_case_val,24*3)[,value_column])), value_column)
    return(score)
}


optimize_linear_model <- function(params, nclasses_per_feature, min_per_feature, max_per_feature, class_per_feature, names_per_feature,
                                  df_case_train, df_case_val, value_column, formula_features, by_s){

    # Train the model considering the params argument
    params <- decodeValueFromBin(params, class_per_feature, nclasses_per_feature, min_per_feature, max_per_feature)
    # params <- c()
    # params$n <- 12
    # params$m <- 0
    # params$alpha_t <- 0.6
    # params$alpha_w <- 0.6
    # params$alpha_g <- 0.6
    # params$tbal <- 16
    # params$p <- 1
    # params <- unlist(params)
    names(params) <- names_per_feature
    mod <- linear_model(n = params["n"], m = params["m"], alpha_t = params["alpha_t"], alpha_w = params["alpha_w"],
                      alpha_g = params["alpha_g"], tbal = params["tbal"], p = params["p"],
                      df_case_train = df_case_train, value_column = value_column,
                      features_formula = formula_features, by_s = by_s)


    mod_lst = list("model"=mod)
    if(by_s == T){
        prediction <- predict_mod_by_s(mod_lst, head(df_case_val, 24*3))$pred
    }else{
        prediction <- predict_mod_all(mod_lst, head(df_case_val,24*3))$pred
    }
    # # Validation dataset changes
    # df_case_val <- tune_inputs(n = params["n"], m = params["m"], alpha_t = params["alpha_t"], alpha_w = params["alpha_w"],
    #                          alpha_g = params["alpha_g"], tbal = params["tbal"], p = params["p"],
    #                          df = df_case_val, value_column = value_column)
    #
    # # Consumption prediction
    # prediction <- predict(mod,newdata=df_case_val)

    # Calculate the score
    score <- calculate_fitness(mod, data.frame(exp(prediction), exp(head(df_case_val,24*3)[,value_column])), value_column)
    return(score)
}

###  AUXILIAR FUNCTIONS FOR CLUSTERING  ----

normalize_range_int <- function(x,inf,sup,threshold_for_min=NULL){
    if(sup>inf){
        if (max(x)==min(x)){
            rep(inf,length(x))
        } else {
            r <- ((x-min(x))/(max(x)-min(x)))*(sup-inf)+inf
            if(!is.null(threshold_for_min)){
                r <- ifelse(x>=threshold_for_min,r,inf)
            }
            return(r)
        }
    }
}

normalize_perc_cons_day <- function(df){
    return(
        as.data.frame(
            t(
                mapply(
                    function(i){as.numeric(df[i,]/sum(df[i,]))},
                    1:nrow(df)
                )
            )
        )
    )
}


normalize_znorm <- function(df, spec_mean_sd=NULL){
    if (is.null(spec_mean_sd)){
        return(
            as.data.frame(
                mapply(
                    function(i){(as.numeric(df[,i])-mean(df[,i],na.rm=T))/sd(df[,i],na.rm=T)},
                    1:ncol(df)
                )
            )
        )
    } else {
        return(
            as.data.frame(
                mapply(
                    function(i){(as.numeric(df[,i])-spec_mean_sd["mean",i])/spec_mean_sd["sd",i]},
                    1:ncol(df)
                )
            )
        )
    }
}


update_str_change<-function(df,results_clustering,time.x="time",time.y="time"){
    if(sum(colnames(df)=="s")>0) df <- df[,-(which(colnames(df)=="s"))]
    df <- merge(df, results_clustering[,c(time.y,"dayhour","s")], by.x = time.x, by.y = time.y)
    return(df)
}


### Clustering and classification algorithms ----

GaussianMixtureModel_clustering<-function(df,value_column="value",k=NULL, tz="UTC"){
    Sys.setenv(TZ=tz)
    df$date <- as.Date(df$time,tz=tz)
    df_agg<-aggregate(as.numeric(df[,value_column]),by=list(
    strftime(strptime(df$time,format="%Y-%m-%d %H:%M:%S",tz=tz),format = "%Y-%m-%d %H",tz=tz)),
    FUN=sum
  )
  df_agg<-data.frame(
    "time"= as.POSIXct(as.character(df_agg$Group.1),format="%Y-%m-%d %H",tz=tz),
    "day"=as.Date(as.POSIXct(as.character(df_agg$Group.1),format="%Y-%m-%d %H",tz=tz),tz=tz),
    "value"=df_agg$x,
    "dayhour"=sprintf("%02i:00",as.integer(substr(df_agg$Group.1,12,13))),
    stringsAsFactors = F
  )

    df_agg <- delete_days_with_no_usage(
        df = df_agg, value_column = "value", time_column = "time",
        days_to_delete = detect_days_with_no_usage(df_agg, "value", "time",tz=tz), tz=tz
    )
    df_spread<-spread(df_agg[c("day","value","dayhour")],"dayhour","value")
    days <- df_spread[,1]
    df_spread<-df_spread[,-1]

    # percentage of consumption in the day
    df_spread_norm_pre<- normalize_perc_cons_day(df_spread)
    # znorm
    df_spread_norm<- normalize_znorm(df_spread_norm_pre)
    #df_spread_norm<- normalize_znorm(df_spread)


    # Generate the final spreated dataframe normalized
    complete_cases <- complete.cases(df_spread_norm)
    df_spread_norm<- df_spread_norm[complete_cases,]
    if (nrow(df_spread_norm)==0){
         stop("There is not enough data to perform the analytics")
    }
    # Initialize the objects
    # Test a clustering from 2 to 10 groups, if k is NULL (default).
    if(is.null(k)) k=seq(2,12)

    # Clustering model

    mclust_results <- tryCatch( Mclust(apply(df_spread_norm,1:2,as.numeric),G = k, modelNames = c("VVI","VII")),
                                  error = function(e){
                                    Mclust(apply(df_spread_norm,1:2,as.numeric),G = k, modelNames = c("EEI","EII"))
                                  }

    if(is.null(mclust_results)){
        stop("The clustering has errors")
    }
    mclust_results[['training_init']] = df_spread_norm_pre
    mclust_results[['value_column']] = value_column
    clustering <- list("cluster"=predict(mclust_results)$classification,"centers"=t(mclust_results$parameters$mean),"k"=mclust_results$G)

    # Delete those individuals far from its centroid.
    distance <-distmat(apply(df_spread_norm,1:2,as.numeric),clustering$centers)

    df_distances <- data.frame(
        "distance"=mapply(function(r){distance[r,clustering$cluster[r]]},1:nrow(df_spread_norm)),
        "k"=clustering$cluster
    )

    for(j in unique(clustering$cluster)){
        if(max(df_distances[df_distances$k==j,"distance"])>2*quantile(df_distances[df_distances$k==j,"distance"],0.025)){
            df_distances[df_distances$k==j,"k_new"] <- ifelse(
                (df_distances[df_distances$k==j,"distance"]>quantile(df_distances[df_distances$k==j,"distance"],0.975) |
                df_distances[df_distances$k==j,"distance"]<quantile(df_distances[df_distances$k==j,"distance"],0.025)),
                NA,
                j
            )
        } else {
            df_distances[df_distances$k==j,"k_new"] <- j
        }
    }

    # Generate the df_centroids dataset
    clustering_results <- data.frame(
        "day"=days[complete_cases],
        "s"=as.character(df_distances$k_new)
    )
    df_structural = merge(df_agg, clustering_results, by="day")

    df_centroids<-aggregate(df_structural$value, by=list(df_structural$dayhour,df_structural$s),FUN=mean)
    colnames(df_centroids)<-c("dayhour","s","value")

    df_structural$s <- as.character(df_structural$s)

    return(list("df"=df_structural,"clustering"=mclust_results))
}










