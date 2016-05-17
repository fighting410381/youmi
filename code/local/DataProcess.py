# -*- coding:utf-8 -*-
import xlrd,json,sys,xlwt
reload(sys)
sys.setdefaultencoding('utf-8')

""" 用来处理数据"""
def DataProcess():
	pass

	"""
	建立ad_tag_content表，或者app_tag_content文件
	@excel_path 是原始内容的excel路径
	@raw_data_path 是特征数据集中的 app_list 或者 adList 对应的文件
	@result_tag_path 是存放处理后的app_tag 获 ad_tag的位置
	@result
	"""
def TagContent(excel_path,raw_data_path,result_tag_path,result_tag_content_path):
	data = xlrd.open_workbook(excel_path)
	table = data.sheets()[0]
	ads_float = table.col_values(0)[1:] # ads里面的每一个元素是 float类型
	ads_str = []
	for e in ads_float:
		ads_str.append(str(int(e)))
	ads_tag = table.col_values(2)[1:]
	ads_content = table.col_values(3)[1:]
	ads_test0510_file = open(raw_data_path)
	ads_test0510 = json.loads(ads_test0510_file.readline())
	ads_test0510_set = set(ads_test0510)
	ads_test0510_tag = open(result_tag_path,'a')
	ads_test0510_tag_content = open(result_tag_content_path,'a')
	common_ads = 0
	for e in range(len(ads_str)):
		if ads_str[e] in ads_test0510_set:
			common_ads += 1
			ads_test0510_tag.write(ads_str[e]+'\t'+ads_tag[e]+'\n')
			ads_test0510_tag_content.write(ads_str[e]+'\t'+ads_tag[e]+'\t'+','.join(ads_content[e].replace('\n','').split('\r'))+'\n')#注意换行符在不同环境下的表示:\n,\r
	ads_test0510_tag.close()
	ads_test0510_tag_content.close()
	"""
	建立ad_tag_content表，或者app_tag_content文件
	@excel_path 是原始内容的excel路径
	@raw_data_path 是特征数据集中的 app_list 或者 adList 对应的文件
	@result_tag_path 是存放处理后的app_tag 获 ad_tag的位置
	@result_tag_content_path 是存放处理后的app_tag_content或者ad_tag_content
	"""
def buildIDTag():
	excel_path = r'c:\12\ads0425\app_description_20160426.xlsx'
 	raw_data_path = r'c:\12\ads0425\apps_test0510.txt'
 	result_tag_path =  r'g:\youmiProject\youmi\result\apps_test0510_tag.txt'
 	result_tag_content_path = r'g:\youmiProject\youmi\result\apps_test0510_tag_content.txt'
 	TagContent(excel_path,raw_data_path,result_tag_path,result_tag_content_path)
def addFeature():
	pass
def writeExcel():
	hour_top10_app_file = open(r'g:\youmiProject\youmi\result\hour_top10_app.txt')
	workbook = xlwt.Workbook(encoding='utf-8')
	worksheet = workbook.add_sheet('hour_top10_app')
	num = 0
	gap =0 # 为了实现换行
	for line in open(r'g:\youmiProject\youmi\result\hour_top10_app.txt'):
		result = line.rstrip().split('\t')
		temp = result[3]
		result[3] = result[2]
		result[2] = temp
		for j,item in enumerate(result):
			if j == 2:
				worksheet.write(j+4*gap,num%10,str(item))
			else:
				worksheet.write(j+4*gap,num%10,item)
		num  += 1
		gap = num/10
	workbook.save(r'g:\youmiProject\youmi\result\hour_top10_app.xlsx')
	hour_top10_app_file.close()


if __name__ == '__main__':
	writeExcel()