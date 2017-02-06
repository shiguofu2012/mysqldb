#coding=utf-8
#!/usr/bin/python
import xlwt

def write_excel(headers, key_order, data, lengths=None):
    '''
    headers: the dict, key is the head name of the table, value is the 
             
    '''
    f = xlwt.Workbook(encoding="utf-8")
    sheet = f.add_sheet('result')
    key = []
    i = 0
    j = 0
    for head in key_order:
	if lengths:
	    length = lengths.get(head, 1000)
	    sheet.col(j).width = length
        sheet.write(0, i, head.encode("utf-8", 'ignore'))
        key.append(headers.get(head))
        i += 1
	j += 1
    i = 1
    for d in data:
        for j in range(len(key)):
            w_data = d.get(key[j])
            if isinstance(w_data, unicode):
                w_data = w_data.encode("utf-8", 'ignore')
            sheet.write(i, j, w_data)
        i += 1
    f.save('result.xls')

if __name__ == "__main__":
    header = {u"昵称": "nickname", u"余额": "account", u"关注": "subscribe"}
    key_order = [u"昵称", u"余额", u"关注"]
    data = [{"nickname": 'gfshi', 'account': 10, 'subscribe': 'N'}]
    write_excel(header, key_order, data)
    #data = get_meme()
    #header = {"taskid": "taskid", "title": "title", u"用户昵称": "nickname", u"收红包个数": "count", u"红包金额": "total"}
    #key_order = ["taskid", "title", u"用户昵称", u"收红包个数", u"红包金额"]
    #write_excel(header, key_order, data)
