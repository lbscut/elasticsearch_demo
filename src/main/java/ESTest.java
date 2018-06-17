
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by lb on 2018/6/11.
 */
public class ESTest {

    static final String IP = "192.168.204.128";
    static final String CLUSTER_NAME = "elasticsearch";
    static final int PORT = 9300;
    static final String INDEX_NAME = "music_index";
    static final String TYPE_NAME = "track";
    
    static final String DATA_PATH = "\\src\\main\\resources\\data";
    static final String MAPPING_PATH = "\\src\\main\\resources\\mapping";
    
    static TransportClient transportClient;

    //添加文档
    public static void addTrack(String json){
    	transportClient.prepareIndex(INDEX_NAME,TYPE_NAME).setSource(json,XContentType.JSON).execute().actionGet();
    }
    
    //添加文档
    public static void addTrack(int id,String json){
    	transportClient.prepareIndex(INDEX_NAME,TYPE_NAME,String.valueOf(id)).setSource(json,XContentType.JSON).execute().actionGet();
    }
    
    //批量添加文档
    public static void batchAddTrack(List<JSONObject> list){
    	BulkRequestBuilder bulkRequst = transportClient.prepareBulk();
    	for (JSONObject obj: list) {
        	IndexRequestBuilder request = transportClient.prepareIndex(INDEX_NAME,TYPE_NAME,obj.getString("id"))
        			.setSource(obj.toJSONString(),XContentType.JSON);
        	bulkRequst.add(request);
		}
    	bulkRequst.execute().actionGet();
    }

    //删除文档
    public static void deleteTrack(String id){
    	transportClient.prepareDelete(INDEX_NAME, TYPE_NAME, id).execute().actionGet();
    }

    //删除整个索引
    public static void deleteIndex(){
    	transportClient.admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
    }
    
    //添加索引
    public static void addIndex(String mapping){
    	transportClient.admin().indices().prepareCreate(INDEX_NAME).addMapping(TYPE_NAME, mapping, XContentType.JSON).execute().actionGet();
    }
    
    public static boolean existIndex() {
    	IndicesExistsResponse response = transportClient.admin().indices().prepareExists(INDEX_NAME).execute().actionGet();
    	return response.isExists();
    }

    //模糊匹配标题
    public static void searchTrackByTitle(String keyword){
    	QueryBuilder queryBuilder = QueryBuilders.matchQuery("title", keyword);
    	System.out.println("search title:" + keyword);
		searchTrack(queryBuilder);
    }

    //获取最新的歌曲
    public static void searchNewestTrack(int count) {
    	SearchResponse response = transportClient.prepareSearch(INDEX_NAME).addSort("time", SortOrder.DESC).setSize(count).execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
    }
    
    //短语匹配作者
    public static void searchTrackByAuthor(String author){
    	QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("author", author);
    	System.out.println("search author:" + author);
		searchTrack(queryBuilder);
    }
    
    //模糊匹配所有字段
    public static void searchTrackByAll(String keyword) {
    	QueryBuilder queryBuilder = QueryBuilders.queryStringQuery(keyword);
    	System.out.println("search all:" + keyword);
		searchTrack(queryBuilder);
    }
    
    //精确搜索评论为0的歌曲
    public static void searchTrackWithoutComment() {
    	QueryBuilder queryBuilder = QueryBuilders.termQuery("comment", 0);
    	System.out.println("search comment:" + 0);
		searchTrack(queryBuilder);
    }
    
    //范围匹配评论数
    public static void searchTrackWithMoreComment(int base) {
    	RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("comment");
    	queryBuilder.gt(base);
    	System.out.println("search comment more than:" + base);
		searchTrack(queryBuilder);
    }
    
    //范围匹配评论数
    public static void searchTrackWithTime(String base) {
    	RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("time");
    	queryBuilder.gt(base);
    	System.out.println("search time more than:" + base);
		searchTrack(queryBuilder);
    }
    
    //多条件匹配，搜索某作者的某歌曲
    public static void searchTrackByTitleAndAuthor(String keyword,String author){
    	QueryBuilder titleQueryBuilder = QueryBuilders.matchQuery("title", keyword);
    	QueryBuilder authorQueryBuilder = QueryBuilders.matchQuery("author", author);
    	BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
    	queryBuilder.must(titleQueryBuilder);
    	queryBuilder.must(authorQueryBuilder);
    	System.out.println("search title:" + keyword + " author:" + author);
		searchTrack(queryBuilder);
    }
    
    
    //根据指定QueryBuilder搜索
    public static void searchTrack(QueryBuilder queryBuilder) {
    	SearchResponse response = transportClient.prepareSearch(INDEX_NAME).setQuery(queryBuilder).execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
    }

    public static void init(){
        try{
            InetAddress inetAddress = InetAddress.getByName(IP);
            InetSocketTransportAddress inetSocketTransportAddress = 
            		new InetSocketTransportAddress(InetAddress.getByName(IP), PORT);
            Settings settings = Settings.builder().put("cluster.name", CLUSTER_NAME).build();
            transportClient = new PreBuiltTransportClient(settings);
            transportClient.addTransportAddress(inetSocketTransportAddress);
        }catch (Exception e){
        	e.printStackTrace();
        }

    }
    
    public static void ingest() {
    	try {
    		if(!existIndex()) {
    			File file = new File(System.getProperty("user.dir") + MAPPING_PATH);
    			FileReader fr = new FileReader(file);
    			BufferedReader br = new BufferedReader(fr);
    			StringBuilder sb = new StringBuilder();
    			String temp;
    			while((temp = br.readLine()) != null) {
     				sb.append(temp);
    			}
    			addIndex(sb.toString());
    		}
    		
    		
			File file = new File(System.getProperty("user.dir") + DATA_PATH);
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String input;
			List<JSONObject> list = new ArrayList<JSONObject>();
			while((input = br.readLine()) != null) {
				JSONObject obj = JSONObject.parseObject(input);
				list.add(obj);
			}
			batchAddTrack(list);
			br.close();
			fr.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    public static void main(String[] args) {
		init();
//		ingest();//导入数据到索引，刚导入需要稍等一会才能搜到，因为es是近乎实时的。
//		searchTrackByTitle("听儿歌");//模糊搜索不确定的关键字,只搜标题
//		searchTrackByAuthor("疯狂英语");//搜索已知的短语，短语不可被拆开
//		searchTrackByAll("英语");//模糊搜索不确定的关键字，搜所有字段
//		searchTrackWithoutComment();//精确搜索评论为0的歌曲
//		searchTrackWithMoreComment(10);//搜索评论数多于10的歌曲
//		searchTrackByTitleAndAuthor("两只老虎", "宝宝巴士");
//		searchTrackWithTime("2018-01-01 00:00:00");
		searchNewestTrack(5);
//		deleteTrack("1");
//		deleteIndex();
		
	}

}
