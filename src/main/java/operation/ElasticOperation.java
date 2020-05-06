package operation;

import config.ElasticConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.*;


public class ElasticOperation {
    private static RestHighLevelClient restHighLevelClient;
    private static RequestOptions requestOptions = RequestOptions.DEFAULT;
    private static IndexRequest indexRequest;
    private static Map<String, Object> dataMap;
    private static IndexResponse response;

    public synchronized RestHighLevelClient makeConnection() {
        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(ElasticConfig.HOST, ElasticConfig.PORT_ONE, ElasticConfig.SCHEME),
                            new HttpHost(ElasticConfig.HOST, ElasticConfig.PORT_TWO, ElasticConfig.SCHEME)));
        }

        return restHighLevelClient;
    }

    public synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }


    // create index with custom settings function
    public void createIndex() {
        CreateIndexRequest request = new CreateIndexRequest(ElasticConfig.INDEX);
        request.source("{\"settings\":{\"index\":{\"number_of_shards\":1,\"analysis\":{\"analyzer\":{\"trigram\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"lowercase\",\"shingle\"]},\"reverse\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"lowercase\",\"reverse\"]}},\"filter\":{\"shingle\":{\"type\":\"shingle\",\"min_shingle_size\":2,\"max_shingle_size\":3}}}}},\"mappings\":{\"properties\":{\"Code\":{\"type\":\"text\",\"analyzer\":\"parsi\",\"search_analyzer\":\"parsi\"},\"Title\":{\"type\":\"text\",\"analyzer\":\"parsi\",\"search_analyzer\":\"parsi\"},\"body\":{\"type\":\"text\",\"analyzer\":\"parsi\",\"search_analyzer\":\"parsi\"},\"bodySuggest\":{\"type\":\"completion\",\"analyzer\":\"parsi\",\"search_analyzer\":\"parsi\",\"fields\":{\"trigram\":{\"type\":\"text\",\"analyzer\":\"trigram\"},\"reverse\":{\"type\":\"text\",\"analyzer\":\"reverse\"}}},\"date\":{\"type\":\"text\",\"analyzer\":\"parsi\",\"search_analyzer\":\"parsi\"},\"category\":{\"type\":\"text\",\"analyzer\":\"parsi\",\"search_analyzer\":\"parsi\"},\"link\":{\"type\":\"text\"},\"label\":{\"type\":\"text\",\"analyzer\":\"parsi\",\"search_analyzer\":\"parsi\"},\"Subtitle\":{\"type\":\"text\",\"analyzer\":\"parsi\",\"search_analyzer\":\"parsi\"}}}}", XContentType.JSON);

        try {
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertNews(String[] doc) {
        dataMap = new HashMap<String, Object>();
        dataMap.put(ElasticConfig.Code, doc[0]);
        dataMap.put(ElasticConfig.Title, doc[1]);
        dataMap.put(ElasticConfig.body, doc[2]);
        dataMap.put(ElasticConfig.bodySuggest, doc[2]);
        dataMap.put(ElasticConfig.date, doc[3]);
        dataMap.put(ElasticConfig.category, doc[4]);
        dataMap.put(ElasticConfig.link, doc[5]);
        dataMap.put(ElasticConfig.label, doc[6]);
        dataMap.put(ElasticConfig.Subtitle, doc[7]);
        indexRequest = new IndexRequest(ElasticConfig.INDEX)
                .source(dataMap);
        try {
            response = restHighLevelClient.index(indexRequest, requestOptions);
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    public List<Map<String, Object>> getNewsByText(SearchSourceBuilder searchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest(ElasticConfig.INDEX);
        SearchResponse searchResponse = null;
        try {
            searchRequest.source(searchSourceBuilder);
            RequestOptions requestOptions = RequestOptions.DEFAULT;
            searchResponse = restHighLevelClient.search(searchRequest, requestOptions);
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
        List<Map<String, Object>> list = new ArrayList<>();
        if (searchResponse != null) {
            SearchHit[] results = searchResponse.getHits().getHits();
            System.out.println(searchResponse.getHits().getTotalHits().value);
            for (SearchHit hit : results) {

                if (hit.getSourceAsMap() != null) {
                    list.add(hit.getSourceAsMap());
                }
            }
        }
        return list;
    }


    public boolean indexExist(String name) {
        GetIndexRequest request = new GetIndexRequest(name);
        boolean exists = false;
        try {
            exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exists;

    }

    public List<String> autoComplet(String q) {

        String test = "{\n" +
                "    \"query\" : {\n" +
                "        \"match_phrase_prefix\": { \"body\": " + '"' + q + '"' + " }\n" +
                "    },\n" +
                "    \"highlight\" : {\n" +
                "    \t \"order\" : \"score\",\n" +
                "        \"number_of_fragments\" : 1,\n" +
                "        \"fragment_size\" : 50,\n" +
                "        \"fields\" : {\n" +
                "            \"body\" : { \"pre_tags\" : [\"\"], \"post_tags\" : [\"\"]}\n" +
                " \n" +
                "        }\n" +
                "    }\n" +
                "}";

        List<SearchPlugin> lb = new ArrayList<>();
        List<String> pishnahad = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        List<String> list = new ArrayList<>();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, lb);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule
                .getNamedXContents()), null, test)) {
            searchSourceBuilder.parseXContent(parser);
            searchRequest.source(searchSourceBuilder);
            SearchResponse s = restHighLevelClient.search(searchRequest, requestOptions);
            SearchHit[] results = s.getHits().getHits();
            List<String> temp = new ArrayList<>();
            Set<String> temp2 = new HashSet<>();
            if (results.length > 0) {
                for (SearchHit hit : results) {

                    if (hit.getSourceAsMap() != null) {
                        temp.add(hit.getHighlightFields().get(ElasticConfig.body).getFragments()[0].toString());
                    }
                }
                for (String word : temp) {
                    if (word.contains(q)) {
                        temp2.add(word.substring(word.indexOf(q), word.length()));
                    }
                }
                for (String p : temp2) {
                    if (p.length() - p.lastIndexOf(" ") < 4) {
                        pishnahad.add(p.substring(0, p.lastIndexOf(" ")));
                    } else {
                        pishnahad.add(p);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return pishnahad;
    }

    public void searchQuery(String q) {


        String query1 = "{\n" +
                "  \"query\": {\n" +
                "    \"multi_match\" : {\n" +
                "      \"query\":      " + '"' + q + '"' + ",\n" +
                "      \"type\":       \"phrase_prefix\",\n" +
                "      \"fields\":     [ \"Title\", \"Subtitle\" , \"body\" ]\n" +
                "    }\n" +
                "  },\n" +
                "        \"highlight\": {\n" +
                "            \"fields\" : {\n" +
                "                \"Title\" : {},\n" +
                "                 \"Subtitle\": {},\n" +
                "                 \"body\": {}\n" +
                "            }\n" +
                "        }\n" +
                "}";
        List<SearchPlugin> lb = new ArrayList<>();
        String query = "{\"query\":{\"multi_match\":{\"query\":" + '"' + q + '"' + ",\"type\":\"best_fields\",\"fields\":[\"Title\",\"Subtitle\",\"body\"],\"tie_breaker\":0.3,\"minimum_should_match\":\"90%\"}},\n" +
                "        \"highlight\": {\n" +
                "            \"fields\" : {\n" +
                "                \"Title\" : {},\n" +
                "                 \"Subtitle\": {},\n" +
                "                 \"body\": {\"pre_tags\" : [\"\"], \"post_tags\" : [\"\"]}\n" +
                "            }\n" +
                "        }\n,\"suggest\":{\"text\":" + '"' + q + '"' + ", \"simple_phrase1\" : {\n" +
                "      \"phrase\" : {\n" +
                "        \"field\" :  \"bodySuggest.trigram\",\n" +
                "        \"size\" :   1,\n" +
                "        \"direct_generator\" : [ {\n" +
                "          \"field\" :            \"bodySuggest.trigram\",\n" +
                "          \"suggest_mode\" :     \"always\",\n" +
                "          \"min_word_length\" :  1\n" +
                "        } ],\n" +
                "        \"collate\": {\n" +
                "          \"query\": { \n" +
                "            \"source\" : {\n" +
                "              \"match_phrase\": {\n" +
                "                \"{{field_name}}\" : \"{{suggestion}}\" \n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          \"params\": {\"field_name\" : \"body\"}, \n" +
                "          \"prune\": false \n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "        \"simple_phrase2\" : {\n" +
                "      \"phrase\" : {\n" +
                "        \"field\" :  \"bodySuggest.trigram\",\n" +
                "        \"size\" :   1,\n" +
                "        \"direct_generator\" : [ {\n" +
                "          \"field\" :            \"bodySuggest.reverse\",\n" +
                "          \"suggest_mode\" :     \"always\",\n" +
                "          \"min_word_length\" :  1,\n" +
                "          \"pre_filter\":\"reverse\",\"post_filter\":\"reverse\"\n" +
                "        } ],\n" +
                "        \"collate\": {\n" +
                "          \"query\": { \n" +
                "            \"source\" : {\n" +
                "              \"match_phrase\": {\n" +
                "                \"{{field_name}}\" : \"{{suggestion}}\" \n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          \"params\": {\"field_name\" : \"body\"}, \n" +
                "          \"prune\": false \n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, lb);
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule
                    .getNamedXContents()), null, query1);
            searchSourceBuilder.parseXContent(parser);
            searchRequest.source(searchSourceBuilder);
            searchRequest.indices(ElasticConfig.INDEX);
            SearchResponse s = restHighLevelClient.search(searchRequest, requestOptions);

            SearchHit[] results = s.getHits().getHits();
            if (results.length > 0) {
                for (SearchHit hit : results) {

                    String position;
                    if (hit.getSourceAsMap() != null) {
                        if (hit.getHighlightFields().get(ElasticConfig.Title) != null) {
                            position = "عنوان خبر";
                        } else if (hit.getHighlightFields().get(ElasticConfig.Subtitle) != null) {
                            position = "خلاصه خبر";
                        } else {
                            position = "متن خبر";
                        }
                        System.out.println("title: " + hit.getSourceAsMap().get(ElasticConfig.Title) + " link: " + hit.getSourceAsMap().get(ElasticConfig.link) + " mahal rokhdad: " + position);
                    }
                }
            } else {
                parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule
                        .getNamedXContents()), null, query);
                searchSourceBuilder.parseXContent(parser);
                searchRequest.source(searchSourceBuilder);
                s = restHighLevelClient.search(searchRequest, requestOptions);

                results = s.getHits().getHits();
                PhraseSuggestion phraseSuggestion1 = s.getSuggest().getSuggestion("simple_phrase1");
                PhraseSuggestion phraseSuggestion2 = s.getSuggest().getSuggestion("simple_phrase2");
                if (results.length > 0) {
                    for (SearchHit hit : results) {

                        String position = "";
                        if (hit.getSourceAsMap() != null) {
                            if (hit.getHighlightFields().get(ElasticConfig.Title) != null) {
                                position = "عنوان خبر" + position;
                            }
                            if (hit.getHighlightFields().get(ElasticConfig.Subtitle) != null) {
                                position = "خلاصه خبر" + position;
                            }
                            if (hit.getHighlightFields().get(ElasticConfig.body) != null) {
                                position = "متن خبر" + position;
                            }
                            System.out.println("title: " + hit.getSourceAsMap().get(ElasticConfig.Title) + " link: " + hit.getSourceAsMap().get(ElasticConfig.link) + " mahal rokhdad: " + position);
                        }
                    }
                }else {
                    System.out.println("moredi yaft nashod!!!!!");
                }
                if (phraseSuggestion1.getEntries().get(0).getOptions().size() > 0 || phraseSuggestion2.getEntries().get(0).getOptions().size() > 0) {
                    System.out.println("query vared kardeh ghalat ast. momken ast kalame zir mored nazar bashad.");
                    String correct;
                    if (phraseSuggestion1.getEntries().get(0).getOptions().size() == 0) {
                        correct = phraseSuggestion2.getEntries().get(0).getOptions().get(0).getText().toString();
                    } else if (phraseSuggestion2.getEntries().get(0).getOptions().size() == 0) {
                        correct = phraseSuggestion1.getEntries().get(0).getOptions().get(0).getText().toString();
                    } else if (phraseSuggestion1.getEntries().get(0).getOptions().get(0).getScore() > phraseSuggestion2.getEntries().get(0).getOptions().get(0).getScore()) {
                        correct = phraseSuggestion1.getEntries().get(0).getOptions().get(0).getText().toString();
                    } else {
                        correct = phraseSuggestion2.getEntries().get(0).getOptions().get(0).getText().toString();

                    }

                    System.out.println(correct);
                }else {

                    if (results.length > 0) {
                        String[] a = q.split(" ");
                        if (results[0].getHighlightFields().get(ElasticConfig.body) != null) {
                            String[] b = results[0].getHighlightFields().get(ElasticConfig.body).getFragments()[0].toString().split(" ");
                            for (int i = 0; i < a.length; i++) {
                                if (results[0].getHighlightFields().get(ElasticConfig.body).getFragments()[0].toString().contains(a[i])) {
                                    List<String> tmp = Arrays.asList(b);
                                    int o = tmp.indexOf(a[i]);
                                    String[] w = Arrays.copyOfRange(b, o, o + a.length - i);
                                    String[] x = null;
                                    if (o > 0) {
                                        x = Arrays.copyOfRange(b, o - i, o);
                                    }
                                    String correct = "";
                                    for (String post : w) {
                                        correct = correct + post + " ";
                                    }
                                    if (x != null) {
                                        if (x.length > 0) {
                                            for (int k = x.length - 1; k > -1; k--) {
                                                correct = x[k] + " " + correct;
                                            }

                                        }
                                    }

                                    w = ElasticOperation.displayTokenUsingStandardAnalyzer(correct);
                                    correct = "";
                                    for (String post : w) {
                                        correct = correct + post + " ";
                                    }
                                    System.out.println("query vared kardeh ghalat ast. momken ast kalame zir mored nazar bashad.");
                                    System.out.println(correct);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void searchByPrefixText(String q) {

        String query = "{\"query\":{\"match_phrase_prefix\":{\"body\":{\"query\":" + '"' + q + '"' + "}}}}";
        List<SearchPlugin> lb = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, lb);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule
                .getNamedXContents()), null, query)) {
            searchSourceBuilder.parseXContent(parser);
            searchRequest.source(searchSourceBuilder);
            SearchResponse s = restHighLevelClient.search(searchRequest, requestOptions);

            SearchHit[] results = s.getHits().getHits();
            for (SearchHit hit : results) {

                if (hit.getSourceAsMap() != null) {
                    System.out.println("title: " + hit.getSourceAsMap().get(ElasticConfig.Title) + " link: " + hit.getSourceAsMap().get(ElasticConfig.link));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public  static String[] displayTokenUsingStandardAnalyzer(     String text) throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withGlobalAnalyzer("standard", text);
        AnalyzeResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.indices().analyze(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
        if (getResponse != null) {
            List<AnalyzeResponse.AnalyzeToken> analyzeTokens = getResponse.getTokens();
            final List<String> terms = new ArrayList<>();
            for (AnalyzeResponse.AnalyzeToken a : analyzeTokens
                    ) {
                terms.add(a.getTerm());
            }
            String[] myArray = new String[terms.size()];
            terms.toArray(myArray);
            return myArray;
        }
        String[] tmp=new String[0];
        return tmp;
    }
}
