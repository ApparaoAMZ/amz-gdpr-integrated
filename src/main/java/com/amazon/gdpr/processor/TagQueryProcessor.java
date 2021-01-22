package com.amazon.gdpr.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprInputFetchDaoImpl;
import com.amazon.gdpr.model.gdpr.input.ImpactTable;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.util.GlobalConstants;

@Component
public class TagQueryProcessor {
	
	public static String CURRENT_CLASS = GlobalConstants.CLS_TAGQUERYPROCESSOR;
	
	@Autowired
	GdprInputFetchDaoImpl gdprInputFetchDaoImpl;
	
	public String tagQueryProcessStatus = "";
	RunErrorMgmt runErrorMgmt=null;
	
	public List<RunSummaryMgmt> updateSummaryQuery (long runId, List<RunSummaryMgmt> lstRunSummaryMgmt) {
		String CURRENT_METHOD = "updateSummaryQuery";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		List<RunSummaryMgmt> tagUpdatedRunSummaryMgmt = new ArrayList<RunSummaryMgmt>();
		
		Map<String, ImpactTable> mapImpactTable = fetchImpactTableMap(runId);
		Map<String, String> mapSummaryInputs = new HashMap<String, String>();
		for(RunSummaryMgmt runSummaryMgmt : lstRunSummaryMgmt) {
			String tableName = runSummaryMgmt.getImpactTableName();
			mapSummaryInputs = new HashMap<String, String>();
			mapSummaryInputs.put(GlobalConstants.KEY_CATEGORY_ID, String.valueOf(runSummaryMgmt.getCategoryId()));
			mapSummaryInputs.put(GlobalConstants.KEY_COUNTRY_CODE, runSummaryMgmt.getCountryCode());
			runSummaryMgmt.setTaggedQueryLoad(fetchCompleteTaggedQuery(tableName, mapImpactTable, mapSummaryInputs));
			tagUpdatedRunSummaryMgmt.add(runSummaryMgmt);
		}
		return tagUpdatedRunSummaryMgmt;
	}
	
	public Map<String, ImpactTable> fetchImpactTableMap(long runId) {
		
		String CURRENT_METHOD = "fetchImpactTableMap";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		Map<String, ImpactTable> mapImpactTable = new HashMap<String, ImpactTable>();
		try{
			List<ImpactTable> lstImpactTable = gdprInputFetchDaoImpl.fetchImpactTable(); 
			for (ImpactTable impactTable : lstImpactTable) {
				mapImpactTable.put(impactTable.getImpactTableName(), impactTable);
			}
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_FETCH_TABLE_DETAIL);
			exception.printStackTrace();
			runErrorMgmt = new RunErrorMgmt(runId, CURRENT_CLASS, CURRENT_METHOD, 
					GlobalConstants.ERR_FETCH_TABLE_DETAIL, exception.getMessage());
		}
		return mapImpactTable;
	}
	
	public String fetchCompleteTaggedQuery(String tableName, Map<String, ImpactTable> mapImpactTable, Map<String, String> mapSummaryInputs) { 
		String CURRENT_METHOD = "fetchCompleteTaggedQuery";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		String taggedCompleteQuery = "";
		
		ImpactTable impactTable = mapImpactTable.get(tableName);
		String tableType = impactTable.getImpactTableType();
		//String parentTableName = impactTable.getParentTable();
		
		String[] aryParent= null;
		String[] aryParentSchema= null;
		String[] aryParentTableCol = null;
		
		if(impactTable.getParentTable().contains(":")) {
			aryParent = impactTable.getParentTable().split(":");
			aryParentSchema = impactTable.getParentSchema().split(":");
			aryParentTableCol = impactTable.getParentTableColumn().split(":");
		}else { 
			aryParent = new String[1];
			aryParentSchema = new String[1];
			aryParentTableCol = new String[1];
			
			aryParent[0] = impactTable.getParentTable();
			aryParentSchema[0] =  impactTable.getParentSchema();
			aryParentTableCol[0] = impactTable.getParentTableColumn();
		}
		
		for(int aryParentIterator=0; aryParentIterator < aryParent.length; aryParentIterator++) { 
			String parentTableNm = aryParent[aryParentIterator];
			String parentSchema = aryParentSchema[aryParentIterator];
			String parentTableCol = aryParentTableCol[aryParentIterator];
			
			String taggedQuery = "";
			String query[] = new String[3];
			query[0] = "SELECT DISTINCT "+tableName+".ID ID, \'"+tableName+"\' IMPACT_TABLE_NAME ";
			query[1] = " FROM " +impactTable.getImpactSchema()+"."+tableName +" "+tableName+", " + 
					parentSchema +"."+parentTableNm+" "+parentTableNm;
			query[2] = " WHERE "+tableName+"."+impactTable.getImpactTableColumn()+" = "+
					parentTableNm+"."+parentTableCol;
			
			ImpactTable currentImpactTable = mapImpactTable.get(parentTableNm); 
			String currentTableType = currentImpactTable.getImpactTableType();
			String currentTableName = currentImpactTable.getImpactTableName();
			
			while(GlobalConstants.TYPE_TABLE_CHILD.equalsIgnoreCase(currentTableType)) {
				query[1] = query[1] + ", "+ currentImpactTable.getParentSchema()+"."+currentImpactTable.getParentTable()+
						" "+currentImpactTable.getParentTable();
				query[2] = query[2] + " AND "+currentTableName+"."+currentImpactTable.getImpactTableColumn()+" = "+
						currentImpactTable.getParentTable()+"."+currentImpactTable.getParentTableColumn();
								
				currentImpactTable = mapImpactTable.get(currentImpactTable.getParentTable());
				currentTableType = currentImpactTable.getImpactTableType();
				currentTableName =  currentImpactTable.getImpactTableName();
			}
			
			if(GlobalConstants.TYPE_TABLE_PARENT.equalsIgnoreCase(currentTableType)) {
				String colNames = currentImpactTable.getImpactColumns();
				String columnNames[];
				if(colNames.contains(GlobalConstants.COMMA_ONLY_STRING))
					columnNames = colNames.split(GlobalConstants.COMMA_ONLY_STRING);
				else {
					columnNames = new String[1];
					columnNames[0] = colNames;
				}
				for(int columnNameIterator = 0; columnNameIterator < columnNames.length; columnNameIterator++) {  
					mapSummaryInputs.get(columnNames[columnNameIterator]);
					query[0] = query[0] +", "+currentTableName +"."+columnNames[columnNameIterator]+" "+columnNames[columnNameIterator];
					query[2] = query[2] +" AND "+currentTableName +"."+columnNames[columnNameIterator]+" = "+
							((columnNames[columnNameIterator].equalsIgnoreCase(GlobalConstants.KEY_CATEGORY_ID)) ? 
									Integer.parseInt(mapSummaryInputs.get(columnNames[columnNameIterator])) : 
										"\'"+mapSummaryInputs.get(columnNames[columnNameIterator])+"\'");
				}				
			}
			taggedQuery = query[0] + query[1] + query[2];
			if(taggedCompleteQuery.equalsIgnoreCase(""))
				taggedCompleteQuery = taggedQuery;
			else
				taggedCompleteQuery = taggedCompleteQuery + " UNION "+ taggedQuery;
		}
		 
		return taggedCompleteQuery;
		
	}
}