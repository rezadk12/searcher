

import com.opencsv.CSVReader;
import config.ElasticConfig;
import operation.ElasticOperation;
import org.apache.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


import static org.elasticsearch.index.query.QueryBuilders.*;

public class ElasticTester {
    private ElasticOperation elasticOperation;
    private String dataDir;
    public static final Logger _log = Logger.getLogger(ElasticTester.class);

    public ElasticTester() {
        ;
        elasticOperation = new ElasticOperation();
        elasticOperation.makeConnection();
    }

    public static void main(String[] args) {

        ElasticTester elasticTester = new ElasticTester();
        Scanner in = new Scanner(System.in);
        try {

            System.out.println();
            if (!elasticTester.existIndex(ElasticConfig.INDEX)) {   // check index exist or not
                elasticTester.createIndex();
                elasticTester.insertNews();
            }
            String[] phrases;
            String s;

            while (true) {
                System.out.println("search bar asas kodam field bashad? adad an ra vared konid");
                System.out.println("1.matn khabar          2.tarikh khabar");
                String option = in.nextLine();
                char op = option.charAt(0);
                switch (op) {
                    case '1':
                        System.out.println("matn query ra vared konid:(baraye estefade az pishnahad query faghat tedadi az caracter an ra vared konid) ");
                        s = in.nextLine();
                        System.out.println("az pishnahad query estefade shavad ya khod query search shavad? (adad an ra vared konid)");
                        System.out.println("1.pishnahad query       2.khod query");
                        op = in.nextLine().charAt(0);
                        if (op == '1') {
                            elasticTester.autoComplete(s);
                        } else if (op == '2') {
                            elasticTester.findNewsByText(s);
                        } else {
                            System.out.println("gozine namotabar vared kardid.");
                        }
                        break;
                    case '2':
                        System.out.println("adad mored nazar ra vared konid: ");
                        s = in.nextLine();
                        elasticTester.findNewsByDate(s);
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void createIndex() {
        elasticOperation.createIndex();
    }

    private void findNewsByText(String txt) {
        try {

            elasticOperation.searchQuery(txt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void findNewsByDate(String txt) {
        try {
            QueryBuilder query = matchPhraseQuery(ElasticConfig.date, txt);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(query);
            searchSourceBuilder.size(100);  // search max size =100
            List<Map<String, Object>> list = elasticOperation.getNewsByText(searchSourceBuilder);
            for (Map<String, Object> map : list) {
                System.out.println("title: " + map.get(ElasticConfig.Title) + " link:" + map.get(ElasticConfig.link));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertNews() throws IOException {
        Scanner in = new Scanner(System.in);
        System.out.println("lotfan adrese file akhbar ra vared konid:");
        dataDir = in.nextLine();
        long startTime = System.currentTimeMillis();
        try {
            File file = new File(dataDir);
            CSVReader reader = null;
            reader = new CSVReader(new FileReader(file));
            String[] line;
            line = reader.readNext();
            while ((line = reader.readNext()) != null) {
                elasticOperation.insertNews(line);
                System.out.println(line[1]);
            }
            long endTime = System.currentTimeMillis();
            System.out.println(" File indexed, time taken: "
                    + (endTime - startTime) + " ms");
        } catch (FileNotFoundException e) {
            System.out.println("adrese file soalat ra dorost vared konid:");
            insertNews();
        }
    }

    private void autoComplete(String query) {
        Scanner in = new Scanner(System.in);
        try {
            List<String> pishnahad = elasticOperation.autoComplet(query);
            if (pishnahad.size() > 0) {
                System.out.println("az bein query haye zir adad gozine marbot ra vared konid:");
                for (int i = 1; i <= pishnahad.size(); i++) {
                    System.out.println(i + "." + pishnahad.get(i - 1));  // print autoComplete options
                }
                Integer option = in.nextInt();
                if (option <= pishnahad.size()) {
                    elasticOperation.searchByPrefixText(pishnahad.get(option - 1));
                } else {
                    System.out.println("gozine namotabar ast.");
                }
            } else {
                System.out.println("pishnahadi vojod nadarad.");
            }
        } catch (Exception e) {

            // e.printStackTrace();
        }
    }

    private boolean existIndex(String name) {
        return elasticOperation.indexExist(name);
    }
}
