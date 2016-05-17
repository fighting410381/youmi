#-*- coding:utf-8 -*-
from pyspark.sql import SQLContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.tree import GradientBoostedTrees
def rfTest(sqlContext,dataset_rdd):
	dataset_positive = dataset_rdd.filter(lambda e:e[1]>0.5)
	dataset_negotive =  dataset_rdd.filter(lambda e:e[1]<0.5)
	train_positive = dataset_positive.sample(False,0.8)
	test_positive = dataset_positive.subtract(train_positive)
	train_negotive = dataset_negotive.sample(False,0.8)
	test_negotive = dataset_negotive.subtract(train_negotive)
	trainset_rdd = train_positive.union(train_negotive)
	testset_rdd = test_positive.union(test_negotive)
	trainset = trainset_rdd.map(lambda e:LabeledPoint(e[1],e[2:]))
	trainset_nums = trainset.count()
	testset = testset_rdd.map(lambda e:LabeledPoint(e[1],e[2:]))
	testset_nums = testset.count()
	trainset_positive = train_positive.count()
	testset_positive = test_positive.count()
	model = RandomForest.trainClassifier(trainset,2,{},3)
	predictions = model.predict(testset.map(lambda x:x.features))
	predict = testset.map(lambda lp: lp.label).zip(predictions)
	hitALL =predict.filter(lambda e:e[0]==e[1]).count()
	hitPositive = predict.filter(lambda e:e[0]==e[1] and (e[0]>0.5)).count()
	positive = predict.filter(lambda e:e[1]>0.5).count()
	recallPositive = hitPositive/float(testset_positive)
	precision = hitPositive/float(positive)
	accuracy = hitALL/float(testset.count())
	F_Value = 2/(1/precision+1/recallPositive)
	return (trainset_nums,testset_nums,trainset_positive,testset_positive,positive,hitPositive,precision,recallPositive,accuracy,F_Value,model)

def lrTest(sqlContext,dataset_rdd,positive_negotive_rate):
	dataset_positive = dataset_rdd.filter(lambda e:e[1]>0.5)
	dataset_negotive =  dataset_rdd.filter(lambda e:e[1]<0.5)
	train_positive = dataset_positive.sample(False,0.8)
	test_positive = dataset_positive.subtract(train_positive)
	train_negotive = dataset_negotive.sample(False,0.8)
	test_negotive = dataset_negotive.subtract(train_negotive)
	trainset_rdd = train_positive.union(train_negotive)
	testset_rdd = test_positive.union(test_negotive)
	trainset = trainset_rdd.map(lambda e:LabeledPoint(e[1],e[2:]))
	trainset_nums = trainset.count()
	testset = testset_rdd.map(lambda e:LabeledPoint(e[1],e[2:]))
	testset_nums = testset.count()
	trainset_positive = train_positive.count()
	testset_positive = test_positive.count()
	model = LogisticRegressionWithLBFGS.train(trainset,iterations = 100)
	predict = testset.map(lambda p:(p.label,model.predict(p.features)))
	hitALL =predict.filter(lambda e:e[0]==e[1]).count()
	hitPositive = predict.filter(lambda e:e[0]==e[1] and (e[0]>0.5)).count()
	positive = predict.filter(lambda e:e[1]>0.5).count()
	recallPositive = hitPositive/float(testset_positive)
	precision = hitPositive/float(positive)
	accuracy = hitALL/float(testset.count())
	F_Value = 2/(1/precision+1/recallPositive)
	return (trainset_nums,testset_nums,trainset_positive,testset_positive,positive,hitPositive,precision,recallPositive,accuracy,F_Value,model)
def svmTest(sqlContext,dataset_rdd,positive_negotive_rate):
	dataset_positive = dataset_rdd.filter(lambda e:e[1]>0.5)
	dataset_negotive =  dataset_rdd.filter(lambda e:e[1]<0.5)
    train_positive = dataset_positive.sample(False,0.8)
    test_positive = dataset_positive.subtract(train_positive)
    train_negotive = dataset_negotive.sample(False,0.8)
    test_negotive = dataset_negotive.subtract(train_negotive)
    trainset_rdd = train_positive.union(train_negotive)
    testset_rdd = test_positive.union(test_negotive)
	trainset = trainset_rdd.map(lambda e:LabeledPoint(e[1],e[2:]))
	trainset_nums = trainset.count()
	testset = testset_rdd.map(lambda e:LabeledPoint(e[1],e[2:]))
	testset_nums = testset.count()
    trainset_positive = train_positive.count()
    testset_positive = test_positive.count()
	model = SVMWithSGD.train(trainset,iterations = 100)
	predict = testset.map(lambda p:(p.label,model.predict(p.features)))
	hitALL =predict.filter(lambda e:e[0]==e[1]).count()
	hitPositive = predict.filter(lambda e:e[0]==e[1] and (e[0]>0.5)).count()
	positive = predict.filter(lambda e:e[1]>0.5).count()
	recallPositive = hitPositive/float(testset_positive)
	precision = hitPositive/float(positive)
	accuracy = hitALL/float(testset.count())
	F_Value = 2/(1/precision+1/recallPositive)
	return (trainset_nums,testset_nums,trainset_positive,testset_positive,positive,hitPositive,precision,recallPositive,accuracy,F_Value,model)
def processData(sqlContext):
    dataset_label_gender = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/labels_gender*')			
	imeis_ads = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_ads*')
	imeis_aboutTimes = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_about*')
    imeis_apps = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_apps*')
    imeis_prvs = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_prvs*')
    imeis_apns = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_apns*')
    imeis_brds = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_brds*')
    imeis_shsws = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_shsws*')
    imeis_ctys = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_ctys*')
    imeis_first = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_first_*')
    imeis_second = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_second*')
    #imeis_ips = sqlContext.parquetFile('/hadoop/hadoop_/wxq/test0405/imeis_ips*')
	dataset = imeis_second.join(dataset_label_gender,imeis_second.imei==dataset_label_gender.imei).drop(dataset_label_gender.imei)
	dataset = dataset.join(imeis_apps,imeis_apps.imei == dataset.imei).drop(dataset.imei)
    dataset = dataset.join(imeis_first,imeis_first.imei== dataset.imei).drop(dataset.imei)
	dataset = dataset.join(imeis_ads,imeis_ads.imei == dataset.imei).drop(dataset.imei)
	dataset = dataset.join(imeis_aboutTimes,imeis_aboutTimes.imei == dataset.imei).drop(dataset.imei)
	dataset = dataset.join(imeis_brds,imeis_brds.imei == dataset.imei).drop(dataset.imei)
	dataset = dataset.join(imeis_shsws,imeis_shsws.imei == dataset.imei).drop(dataset.imei)
	dataset = dataset.join(imeis_ctys,imeis_ctys.imei == dataset.imei).drop(dataset.imei)
	dataset = dataset.join(imeis_apns,imeis_apns.imei == dataset.imei).drop(dataset.imei)
	dataset = dataset.join(imeis_prvs,imeis_prvs.imei == dataset.imei).drop(dataset.imei)
    dataset_rdd = dataset.map(lambda e:(e.imei,e.label,int(e.gender),e.interests,e.apps,e.first_interests,e.ads,e.brds,e.ctys,e.shsws,e.wifiRate,e.appearDayList,e.appearDays,e.avgTimesPerDay,e.timeDuring)).map(lambda e:changeIntoTuple(e))

    return dataset_rdd
def changeIntoTuple(line):
    result =[]
    for e in line:
        if type(e)== tuple or type(e)==list:
            for a in e:
                result.append(a)
        else:
            result.append(e)
	return tuple(result)
def testManyTimesLR(times,sqlContext,dataset_rdd,positive_negotive_rate):
    positive = 0.0
	hitPositive = 0.0
	precision = 0.0
	recallPositive = 0.0
	accuracy = 0.0
	F_Value = 0.0
	for e in range(times):
        a,b,c,d,e,f,g,h,i,j,k =  lrTest(sqlContext,dataset_rdd,positive_negotive_rate)
		positive += e
		hitPositive += f
		precision += g
		recallPositive += h
		accuracy += i
		F_Value += j
	return(positive/times,hitPositive/times,precision/times,recallPositive/times,accuracy/times,F_Value/times,k)
def testManyTimesSVM(times,sqlContext,dataset_rdd,positive_negotive_rate):
    positive = 0.0
	hitPositive = 0.0
	precision = 0.0
	recallPositive = 0.0
	accuracy = 0.0
	F_Value = 0.0
	for e in range(times):
        a,b,c,d,e,f,g,h,i,j,k =  svmTest(sqlContext,dataset_rdd,positive_negotive_rate)
		positive += e
		hitPositive += f
		precision += g
		recallPositive += h
		accuracy += i
		F_Value += j
	return(positive/times,hitPositive/times,precision/times,recallPositive/times,accuracy/times,F_Value/times,k)
def crossValidator(IterNums,dataset_rdd,rate):
	dataset_positive = dataset_rdd.filter(lambda e:e[1]>0.5)
	dataset_negotive =  dataset_rdd.filter(lambda e:e[1]<0.5)
	# dataset_positive1,dataset_positive2,dataset_positive3,dataset_positive4,dataset_positive5 = dataset_positive.randomSplit([1,1,1,1,1])
	# dataset_negotive1,dataset_negotive2,dataset_negotive3,dataset_negotive4,dataset_negotive5 = dataset_negotive.randomSplit([1,1,1,1,1])
	dataset_positive_list = dataset_positive.randomSplit([1,1,1,1,1])
	dataset_negotive_list = dataset_negotive.randomSplit([1,1,1,1,1])
	result = []
        #result2 = []
	for i in range(5):
		testset_positive = dataset_positive_list[i].count()
		testset_rdd = dataset_positive_list[i].union(dataset_negotive_list[i])
        testset_count = testset_rdd.count()
		trainset_rdd = dataset_rdd.subtract(testset_rdd)
		trainset = trainset_rdd.map(lambda e:LabeledPoint(e[1],e[2:]))
		testset = testset_rdd.map(lambda e:LabeledPoint(e[1],e[2:]))
		model = GradientBoostedTrees.trainClassifier(trainset, {}, numIterations=IterNums,learningRate = rate)
        	#model2 = LogisticRegressionWithLBFGS.train(trainset,iterations = 100)
		predictions = model.predict(testset.map(lambda x:x.features))
                #predictions2 = model2.predict(testset.map(lambda x:x.features))
		predict = testset.map(lambda lp: lp.label).zip(predictions)
                #predict2 = testset.map(lambda lp:lp.label).zip(predictions2)
		hitALL =predict.filter(lambda e:e[0]==e[1]).count()
                #hitALL2 = predict2.filter(lambda e:e[0]==e[1]).count()
		hitPositive = predict.filter(lambda e:e[0]==e[1] and (e[0]>0.5)).count()
                #hitPositive2 = predict2.filter(lambda e:e[0]==e[1] and (e[0]>0.5)).count()
		positive = predict.filter(lambda e:e[1]>0.5).count()
                #positive2 = predict2.filter(lambda e:e[1]>0.5).count()
		recall = hitPositive/float(testset_positive)
                #recall2 = hitPositive2/float(testset_positive)
		precision = hitPositive/float(positive)
                #precision2 = hitPositive2/float(positive2)
		accuracy = hitALL/float(testset_count)
                #accuracy2 = hitALL2/float(testset_count)
		F_Value = 2/(1/precision+1/recall)
                #F_Value2 = 2/(1/precision2+1/recall2)
		result.append((precision,recall,accuracy,F_Value,hitPositive,positive,testset_positive,testset_count))
		#result2.append((precision2,recall2,accuracy2,F_Value2,hitPositive2,positive2,testset_positive,testset_count))
	avg_precision = 0.0
	avg_recall = 0.0
	avg_accuracy = 0.0
	avg_F_Value = 0.0
	avg_hitPositive = 0
	avg_positive = 0
    avg_testset_positive = 0
    avg_testset_count = 0
	for e in result:
		avg_precision += e[0]
		avg_recall += e[1]
		avg_accuracy += e[2]
		avg_F_Value += e[3]
		avg_hitPositive += e[4]
		avg_positive += e[5]
        avg_testset_positive += e[6]
        avg_testset_count += e[7]
	result.append((avg_precision/5,avg_recall/5,avg_accuracy/5,avg_F_Value/5,avg_hitPositive/5,avg_positive/5,avg_testset_positive/5,avg_testset_count/5))
	#avg_precision2 = 0.0
	#avg_recall2 = 0.0
	#avg_accuracy2 = 0.0
	#avg_F_Value2 = 0.0
	#avg_hitPositive2 = 0
	#avg_positive2 = 0
        #avg_testset_positive2 = 0
        #avg_testset_count2 = 0
	#for e in result2:
		#avg_precision2 += e[0]
		#avg_recall2 += e[1]
		#avg_accuracy2 += e[2]
		#avg_F_Value2 += e[3]
		#avg_hitPositive2 += e[4]
		#avg_positive2 += e[5]
                #avg_testset_positive2 += e[6]
                #avg_testset_count2 += e[7]
	#result2.append((avg_precision2/5,avg_recall2/5,avg_accuracy2/5,avg_F_Value2/5,avg_hitPositive2/5,avg_positive2/5,avg_testset_positive2/5,avg_testset_count2/5))
    return result
def randomSplit(dataset_rdd,k):
	dataset_positive = dataset_rdd.filter(lambda e:e[1]>0.5)
	dataset_negotive =  dataset_rdd.filter(lambda e:e[1]<0.5)
    dataset_positive_list = []
    dataset_negotive_list = []
    for i in range(5):
        dataset_positive_list[i] = dataset_positive.sample(False,0.2)
        dataset_negotive_list[i] = dataset_negotive.sample(False,0.2)
        dataset_positive = dataset_positive.subtract(dataset_positive_list[i])
        dataset_negotive = dataset_negotive.subtract(dataset_negotive_list[i])
    return dataset_positive_list,dataset_negotive_list
def createOneHot(lst,dct):
	oneHotCode = [0]*len(dct)
	for e in lst:
		oneHotCode[dct[e]] = 1
	return oneHotCode
	"""PCA belong to Spark.ML,想要转换的列必须是Vectors类型"""
def  dataTransformByPCA(df,dimension,inputColName):
	pca = PCA(k=dimension,inputCol=inputColName,outputCol='pcaFeatures')
	model = pca.fit(df)
	result = model.transform(df)
	return result