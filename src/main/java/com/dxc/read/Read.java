package com.dxc.read;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class Read {
	
	static public final String _ERR_STR = "!ERR";
	
	public static void main(String[] args) {
		AmazonDynamoDB client = AmazonDynamoDBAsyncClientBuilder.standard()
				.withEndpointConfiguration(new AwsClientBuilder
//						 在 eclipse 中安装过 aws 插件的，可以将除此的地区名字设置为 local，在 eclipse 本地中可以表
						.EndpointConfiguration("http://localhost:8000", "local"))
//						.EndpointConfiguration("http://localhost:8000", "ap-northeast-1"))
				.build();
		DynamoDB dynamoDB = new DynamoDB(client);
		
//		 调用 readExcel() 方法
		Map<String, Map<Integer, List<String>>> readExcel = readExcel();
		Set<String> sheetIndexs = readExcel.keySet();
//		 遍历表名
		for (String tableName : sheetIndexs) {
//			每次遍历表名时都需要重新声明一个 item和 primary，不然会导致前表声明的主键在本次中依然可以使用
			Item item = new Item();
			PrimaryKey p = new PrimaryKey();
//			 通过 表名 key 来获取行的信息
			Map<Integer,List<String>> rows = readExcel.get(tableName);
//			 通过行 key 来获取列信息
			Set<Integer> keys = rows.keySet();
//			 遍历行 key ，获取列值
//			for(Integer key:keys) {
//				System.out.println("每列数据:"+rows.get(key));
//			}
			
//			 获取表名
			Table table = dynamoDB.getTable(tableName);
//			获取所遍历表的信息
			TableDescription description = table.describe();
			System.out.println(tableName + "表中信息如下：" + description);
//			声明主键名字和主键类型参数
			List<String> keyAttributeName = new ArrayList<String>();
			List<String> keyType = new ArrayList<String>();
//			 获取到属性信息
			List<AttributeDefinition> attributeDefinitions = description.getAttributeDefinitions();
//			List<KeySchemaElement> attributeDefinitions = description.getKeySchema();
			System.out.println("---" + attributeDefinitions);
			
//			 循环遍历放入主键信息   从dynamodb中获取主键名称及类别
			for (AttributeDefinition a : attributeDefinitions) {
				keyAttributeName.add(a.getAttributeName());
				keyType.add(a.getAttributeType());
			}
			
//			第一行标题行跳过
//			j 控制行， i 控制列
			int k = 1;
			for (int j = 1; j < keys.size(); j++) {
//				 从某行的第一列开始读取数据
				for (int i = 0; i<rows.get(0).size(); i++) {
//					 当只有hashKey时
					int value = 0;
					String s = null;
					if(keyAttributeName.size() == 1) {
//						当只有一个属性并且属性类型为 number 时
						if("N".equals(keyType.get(0))) {
							value = Double.valueOf(rows.get(j).get(0)).intValue();
							p.addComponent(keyAttributeName.get(0), value);
						} else {
//							将默认从excel表中读取的数字后面的小数点之后的数据去掉
//							System.out.println("默认的读取状态是会在后面加小数点" + rows.get(j).get(0));
							s = rows.get(j).get(0).split("\\.")[0];
							p.addComponent(keyAttributeName.get(0), s);
						}
						item.withPrimaryKey(p).with(rows.get(0).get(i), rows.get(j).get(i));
						item.withPrimaryKey(p);
						table.putItem(item);
					}else {
						if("N".equals(keyType.get(0))) {
							value = Double.valueOf(rows.get(j).get(0)).intValue();
							 /**
							  * 特别注意：  
							  * 当是复合主键时，此处p.addComponent(keyAttributeName.get(0), value) 方法不能使用，原因是放到集合中，
							  * 排序键和分区键的位置可能发生了变化导致插入出错
							 */
							p.addComponent(description.getKeySchema().get(0).getAttributeName(), value);
						} else {
							s = rows.get(j).get(0).split("\\.")[0];
							p.addComponent(description.getKeySchema().get(0).getAttributeName(), s);
						}
						if("N".equals(keyType.get(1))) {
							value = Double.valueOf(rows.get(j).get(1)).intValue();
							p.addComponent(description.getKeySchema().get(1).getAttributeName(), value);
						} else {
							s = rows.get(j).get(1).split("\\.")[0];
							p.addComponent(description.getKeySchema().get(1).getAttributeName(), s);
//							System.out.println(rows.get(0).get(i) + ":" + rows.get(j).get(i));
						}
						item.withPrimaryKey(p).with(rows.get(0).get(i), rows.get(j).get(i));
//						 当有主键类型为N时，我们需要在加上这个 item.withPrimaryKey(p);
						item.withPrimaryKey(p);
						table.putItem(item);
					}
				}
			}
		}
	}
	
//	 解析excel表
	public static Map<String,Map<Integer,List<String>>> readExcel() {
//		 还用循环嵌套来读取数据
		Map<String, Map<Integer, List<String>>> sheetsMap = new HashMap<String, Map<Integer,List<String>>>();
		try {
//			 文件读取路径
			InputStream is = new FileInputStream("C:\\Users\\lyue2\\Desktop\\DXC.xlsx");
//			 创建工作簿
			Workbook wb = new XSSFWorkbook(is);
//			 检查sheet表格有多少个。从0开始计数，真实数量需要加1。获取真实sheet个数：wb.getNumberOfSheets()
			int sheetcount = wb.getActiveSheetIndex();
//			int num = wb.getNumberOfSheets();
//			System.out.println("表个数：" + num);
			for (int i = 0; i <= sheetcount; i++) {
//				获取到第i个sheet
				Sheet sheet = wb.getSheetAt(i);
//				 获取表名
				String tableName = sheet.getSheetName();
				System.out.println("获取的到表名有： " + tableName);
				Map<Integer, List<String>> map = readRowFromExcel(sheet);
				sheetsMap.put(tableName, map);
			}
			wb.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sheetsMap;
	}
	
//	 解析sheet里面的行信息
	public static Map<Integer, List<String>> readRowFromExcel(Sheet sheet){
		Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
//		 迭代遍历单个sheet的行数
		Iterator<Row> rowIter = sheet.rowIterator();
		int rowNum = 0;
		while(rowIter.hasNext()) {
			Row row = rowIter.next();
			List<String> cellList = getRowValue(rowNum,row);
			map.put(rowNum, cellList);
			rowNum ++;
		}
		return map;
	}
	
//	 解析行里面的列信息
	public static List<String> getRowValue(int rowNum,Row row){
		List<String> list = new ArrayList<String>();
		Iterator<Cell> cellIter = row.cellIterator();
		while(cellIter.hasNext()) {
			Cell cell = cellIter.next();
//			 获取表格中数据的类型
			CellType cellType = cell.getCellTypeEnum();
			if (cellType == CellType.BLANK) {
				list.add(null);
			} else if (cellType == CellType.BOOLEAN) {
				System.out.println("boolean");
				Boolean bl = cell.getBooleanCellValue();
				list.add(bl.toString());
			} else if (cellType == CellType.ERROR) {
				System.out.println("error");
				list.add(_ERR_STR);
			} else if (cellType == CellType.FORMULA) {
				System.out.println("formula");
				list.add(_ERR_STR);
			} else if (cellType == CellType.NUMERIC) {
				if (DateUtil.isCellDateFormatted(cell)) {
					Date dt = cell.getDateCellValue();
					if (dt != null) {
						list.add(dt.toString());
					} else {
						list.add("");
					}
				} else {
					Double db = cell.getNumericCellValue();
					list.add(db.toString());
				}
			} else if (cellType == CellType.STRING) {
				list.add(cell.getStringCellValue());
			} else {
				list.add("");
			}
		}
		return list;
	}

}
