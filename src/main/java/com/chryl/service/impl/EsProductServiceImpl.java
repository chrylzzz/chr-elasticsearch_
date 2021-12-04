package com.chryl.service.impl;

import com.chryl.dao.EsProductDao;
import com.chryl.domain.EsProduct;
import com.chryl.domain.EsProductRelatedInfo;
import com.chryl.repository.EsProductRepository;
import com.chryl.service.EsProductService;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.awt.print.Book;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * 商品搜索管理Service实现类: 注释理解版本
 * Created by Chr.yl on 2018/6/19.
 */
@Service
public class EsProductServiceImpl implements EsProductService {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsProductServiceImpl.class);
    @Autowired
    private EsProductDao productDao;
    @Autowired
    private EsProductRepository productRepository;
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

//    //使用@Query注解可以用Elasticsearch的DSL语句进行查询
//    @Query("{"bool" : {"must" : {"field" : {"name" : " ? 0"}}}}")
//    Page<EsProduct> findByName(String name, Pageable pageable);


    @Override
    public int importAll() {
        //得到db所有的商品
        List<EsProduct> esProductList = productDao.getAllEsProductList(null);
        //全部存入es
        Iterable<EsProduct> esProductIterable = productRepository.saveAll(esProductList);
        Iterator<EsProduct> iterator = esProductIterable.iterator();
        int result = 0;
        while (iterator.hasNext()) {
            result++;
            iterator.next();
        }
        return result;
    }

    @Override
    public void delete(Long id) {
        //从es删除
        productRepository.deleteById(id);
    }

    @Override
    public EsProduct create(Long id) {
        EsProduct result = null;
        //db得到商品
        List<EsProduct> esProductList = productDao.getAllEsProductList(id);
        if (esProductList.size() > 0) {
            EsProduct esProduct = esProductList.get(0);
            //存入es
            result = productRepository.save(esProduct);
        }
        return result;
    }

    @Override
    public void delete(List<Long> ids) {
        if (!CollectionUtils.isEmpty(ids)) {
            List<EsProduct> esProductList = new ArrayList<>();
            for (Long id : ids) {
                EsProduct esProduct = new EsProduct();
                esProduct.setId(id);
                esProductList.add(esProduct);
            }
            //从es中批量删除
            productRepository.deleteAll(esProductList);
        }
    }

    @Override
    public Page<EsProduct> search(String keyword, Integer pageNum, Integer pageSize) {
        //先配置Page信息
        Pageable pageable = PageRequest.of(pageNum, pageSize);
        //注意keyword 为所有查询的参数
        return productRepository.findByNameOrSubTitleOrKeywords(keyword, keyword, keyword, pageable);
    }

    @Override
    public Page<EsProduct> search(String keyword, Long brandId, Long productCategoryId, Integer pageNum, Integer pageSize, Integer sort) {
        Pageable pageable = PageRequest.of(pageNum, pageSize);
        //1.NativeSearchQueryBuilder构建查询 需要将匹配到的结果字符进行高亮显示
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        //分页
        nativeSearchQueryBuilder.withPageable(pageable);
        //过滤
        if (brandId != null || productCategoryId != null) {//如果查询的是id,那么就精确查询
//    matchAllQuery()方法用来匹配全部文档,没有查询条件：QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();//搜索全部文档
//    matchQuery(String name,Object  text)：词条匹配，先分词然后在调用termQuery进行匹配，匹配单个字段，匹配字段名为filedname,值为value的文档：//单个匹配，搜索name为jack的文档：QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "jack");
//    multiMatchQuery(Object text, String... fieldNames)多个字段匹配某一个值：QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("music","name", "interest");//搜索name中或interest中包含有music的文档（必须与music一致）
//    TermQuery：词条匹配，不分词
//    wildcardQuery：通配符匹配，模糊查询：WildcardQueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name", "*jack*");//搜索名字中含有jack文档（name中只要包含jack即可）
//    fuzzyQuery：模糊匹配
//    rangeQuery：范围匹配
//    booleanQuery：布尔查询，进行复合查询，可以使用must(相当于and)，should(相当于or)
            //2.设置QueryBuilder,为简单条件查询，该处使用布尔查询,多条件查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (brandId != null) {
                /*组合查询BoolQueryBuilder:builder下有must、should以及mustNot 相当于sql中的and、or以及not
                 * must(QueryBuilders)   :AND
                 * mustNot(QueryBuilders):NOT
                 * should(QueryBuilders):OR
                 */
                //1.termQuery 精确查询
                boolQueryBuilder.must(QueryBuilders.termQuery("brandId", brandId));//相当于and,可以直接QueryBuilders进行构建查询或者使用如下方式：
                /* 第二种方式使用must
                //2.wildcardQuery 模糊查询
                WildcardQueryBuilder queryBuilder1 = QueryBuilders.wildcardQuery("name", "*jack*");//搜索名字中含有jack的文档
                WildcardQueryBuilder queryBuilder2 = QueryBuilders.wildcardQuery("interest", "*read*");//搜索interest中含有read的文档
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                //name中必须含有jack,interest中必须含有read,相当于and
                boolQueryBuilder.must(queryBuilder1);
                boolQueryBuilder.must(queryBuilder2);*/
            }
            if (productCategoryId != null) {
                boolQueryBuilder.must(QueryBuilders.termQuery("productCategoryId", productCategoryId));
            }
            //fuzzyQuery 设置模糊搜索,有学习两个字
//            builder.must(QueryBuilders.fuzzyQuery("sumary", "学习"));
            //设置要查询的内容中含有关键字
//            builder.must(new QueryStringQueryBuilder("man").field("springdemo"));
            nativeSearchQueryBuilder.withFilter(boolQueryBuilder);
        }
        //搜索:matchQuery 分词查询，--这里可以设置分词器,采用默认的分词器
        if (StringUtils.isEmpty(keyword)) {
            //相当于就没有设置查询条件
            nativeSearchQueryBuilder.withQuery(QueryBuilders.matchAllQuery());
        } else {
            //不分词查询，参数1： 字段名，参数2：多个字段查询值,因为不分词，所以汉字只能查询一个字，英语是一个单词.
            //QueryBuilder queryBuilder = QueryBuilders.termsQuery("fieldName", "fieldlValue1", "fieldlValue2...");
            //分词查询，采用默认的分词器
            //QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("fieldlValue", "fieldName1", "fieldName2", "fieldName3");
            List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilders = new ArrayList<>();
            //name , subTitle , keyword : 全部匹配分词查询,找到可以匹配的数据,因为输入的keyword不知道包含什么信息,所以全部匹配
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("name", keyword),//matchQuery 分词查询
                    /**
                     * 它会在查询结束后对每一个匹配的文档进行一系列的重打分操作，最后以生成的最终分数进行排序,此处权重分配
                     * weight
                     weight 的用法最为简单，只需要设置一个数字作为权重，文档的分数就会乘以该权重。
                     他最大的用途应该就是和过滤器一起使用了，因为过滤器只会筛选出符合标准的文档，而不会去详细的计算每个文档的具体得分，所以只要满足条件的文档的分数都是 1，而 weight 可以将其更换为你想要的数值。
                     注意有多种计算分值的函数
                     */
                    ScoreFunctionBuilders.weightFactorFunction(10)));
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("subTitle", keyword),
                    ScoreFunctionBuilders.weightFactorFunction(5)));
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("keywords", keyword),
                    ScoreFunctionBuilders.weightFactorFunction(2)));
            FunctionScoreQueryBuilder.FilterFunctionBuilder[] builders = new FunctionScoreQueryBuilder.FilterFunctionBuilder[filterFunctionBuilders.size()];
            filterFunctionBuilders.toArray(builders);
            //build query
            FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(builders)
                    .scoreMode(FunctionScoreQuery.ScoreMode.SUM)
                    .setMinScore(2);
            //写入query
            nativeSearchQueryBuilder.withQuery(functionScoreQueryBuilder);
        }
        //排序:将排序设置到构建中
        if (sort == 1) {//按新品从新到旧
            nativeSearchQueryBuilder.withSort(SortBuilders.fieldSort("id").order(SortOrder.DESC));
        } else if (sort == 2) {//按销量从高到低
            nativeSearchQueryBuilder.withSort(SortBuilders.fieldSort("sale").order(SortOrder.DESC));
        } else if (sort == 3) {//按价格从低到高
            nativeSearchQueryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.ASC));
        } else if (sort == 4) {//按价格从高到低
            nativeSearchQueryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        } else {//按相关度
            nativeSearchQueryBuilder.withSort(SortBuilders.scoreSort().order(SortOrder.DESC));
        }
        //自定义排序规则
        nativeSearchQueryBuilder.withSort(SortBuilders.scoreSort().order(SortOrder.DESC));
        //自定义高亮
        HighlightBuilder.Field filed = new HighlightBuilder.Field("*");//设置需要高亮的属性，也可使用通配符*
        filed.preTags("<span style='color:red;'>");//样式前缀
        filed.postTags("</span");//样式后缀
        nativeSearchQueryBuilder.withHighlightFields(filed);//设置高亮，可变参数，可传入多个高亮的属性
//        nativeSearchQueryBuilder.withHighlightFields(
//                new HighlightBuilder.Field("name").preTags("<font color='red'>").postTags("</font>"),
//                new HighlightBuilder.Field("subTitle").preTags("<font color='red'>").postTags("</font>"),
//                new HighlightBuilder.Field("keywords").preTags("<em>").postTags("</em>")
//        );
        //将build query放入search query
        NativeSearchQuery searchQuery = nativeSearchQueryBuilder.build();
        LOGGER.info("DSL:{}", searchQuery.getQuery().toString());
        //高亮未成功
        AggregatedPage<EsProduct> esProducts = elasticsearchTemplate.queryForPage(searchQuery, EsProduct.class, new SearchResultMapper() {////对查询进行自定义封装
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse searchResponse, Class<T> aClass, Pageable pageable) {
                List<EsProduct> esProductList = new ArrayList<>();
                //根据相应结果货期His
                SearchHits hits = searchResponse.getHits();
                //索取his数组
                SearchHit[] searchHits = hits.getHits();
                //遍历结果数据
                for (SearchHit searchHit : searchHits) {

                    EsProduct esProductsMapper = new EsProduct();
                    //获取原始数据
                    Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                    //获取高亮处理后的数据
                    Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();

//                    esProductsMapper.setId(searchHit.getId());
                    esProductsMapper.setSubTitle(sourceAsMap.get("subTitle").toString());
                    if (highlightFields.containsKey("subTitle")) {//是否有高亮处理，有就将原始数据覆盖掉
                        esProductsMapper.setSubTitle(highlightFields.get("subTitle").getFragments()[0].toString());
                    }
                    esProductsMapper.setKeywords(sourceAsMap.get("keywords").toString());
                    if (highlightFields.containsKey("keywords")) {//是否有高亮处理，有就将原始数据覆盖掉
                        esProductsMapper.setKeywords(highlightFields.get("keywords").getFragments()[0].toString());
                    }
                    esProductsMapper.setName(sourceAsMap.get("name").toString());
                    if (highlightFields.containsKey("name")) {//是否有高亮处理，有就将原始数据覆盖掉
                        esProductsMapper.setName(highlightFields.get("name").getFragments()[0].toString());
                    }
                    //数字
//                    book.setPrice(Double.parseDouble(sourceAsMap.get("price").toString()));
//                    if (highlightFields.containsKey("price")) {//是否有高亮处理，有就将原始数据覆盖掉
//                        book.setPrice(Double.parseDouble(highlightFields.get("price").getFragments()[0].toString()));
//                    }
                    //日期
//                    book.setPubdata(new Date(Long.parseLong(sourceAsMap.get("pubdata").toString())));
//                    if (highlightFields.containsKey("pubdata")) {//是否有高亮处理，有就将原始数据覆盖掉
//                        book.setPubdata(new Date(Long.parseLong(highlightFields.get("pubdata").getFragments()[0].toString())));
//                    }
                    esProductList.add(esProductsMapper);
                }
                return new AggregatedPageImpl<>((List<T>) esProductList);
            }
        });
        //执行查询
        return productRepository.search(searchQuery);
    }

    @Override
    public Page<EsProduct> recommend(Long id, Integer pageNum, Integer pageSize) {
        Pageable pageable = PageRequest.of(pageNum, pageSize);
        List<EsProduct> esProductList = productDao.getAllEsProductList(id);
        if (esProductList.size() > 0) {
            EsProduct esProduct = esProductList.get(0);
            String keyword = esProduct.getName();
            Long brandId = esProduct.getBrandId();
            Long productCategoryId = esProduct.getProductCategoryId();
            //根据商品标题、品牌、分类进行搜索,matchQuery分词查询
            List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilders = new ArrayList<>();
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("name", keyword),
                    ScoreFunctionBuilders.weightFactorFunction(8)));
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("subTitle", keyword),
                    ScoreFunctionBuilders.weightFactorFunction(2)));
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("keywords", keyword),
                    ScoreFunctionBuilders.weightFactorFunction(2)));
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("brandId", brandId),
                    ScoreFunctionBuilders.weightFactorFunction(10)));
            filterFunctionBuilders.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("productCategoryId", productCategoryId),
                    ScoreFunctionBuilders.weightFactorFunction(6)));
            FunctionScoreQueryBuilder.FilterFunctionBuilder[] builders = new FunctionScoreQueryBuilder.FilterFunctionBuilder[filterFunctionBuilders.size()];
            filterFunctionBuilders.toArray(builders);
            FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(builders)
                    .scoreMode(FunctionScoreQuery.ScoreMode.SUM)
                    .setMinScore(2);
            NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();
            builder.withQuery(functionScoreQueryBuilder);
            builder.withPageable(pageable);
            NativeSearchQuery searchQuery = builder.build();
            LOGGER.info("DSL:{}", searchQuery.getQuery().toString());//dsl 语句
            return productRepository.search(searchQuery);
        }
        return new PageImpl<>(null);
    }


    /**
     * 聚合搜索:
     * 首先来说下我们的需求，可以根据搜索关键字获取到与关键字匹配商品相关的分类、品牌以及属性，下面这张图有助于理解；
     * 在SpringBoot中实现，聚合操作比较复杂，已经超出了Elasticsearch Repositories的使用范围，需要直接使用ElasticsearchTemplate来实现；
     *
     * @param keyword
     * @return
     */
    @Override
    public EsProductRelatedInfo searchRelatedInfo(String keyword) {
        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();
        //搜索条件
        if (StringUtils.isEmpty(keyword)) {
            //空,匹配所有文件，相当于就没有设置查询条件
            builder.withQuery(QueryBuilders.matchAllQuery());
        } else {
            //分词查询，采用默认的分词器
            builder.withQuery(QueryBuilders.multiMatchQuery(keyword, "name", "subTitle", "keywords"));
        }
        /**
         * 聚合查询:
         * https://www.cnblogs.com/hirampeng/p/10035858.html
         */
        //聚合搜索品牌名称
        builder.addAggregation(AggregationBuilders.terms("brandNames").field("brandName"));
        //集合搜索分类名称
        builder.addAggregation(AggregationBuilders.terms("productCategoryNames").field("productCategoryName"));
        //聚合搜索商品属性，去除type=1的属性
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.nested("allAttrValues", "attrValueList")
                .subAggregation(AggregationBuilders.filter("productAttrs", QueryBuilders.termQuery("attrValueList.type", 1))
                        .subAggregation(AggregationBuilders.terms("attrIds")
                                .field("attrValueList.productAttributeId")
                                .subAggregation(AggregationBuilders.terms("attrValues")
                                        .field("attrValueList.value"))
                                .subAggregation(AggregationBuilders.terms("attrNames")
                                        .field("attrValueList.name"))));
        builder.addAggregation(aggregationBuilder);
        NativeSearchQuery searchQuery = builder.build();
        return elasticsearchTemplate.query(searchQuery, response -> {
            LOGGER.info("DSL:{}", searchQuery.getQuery().toString());
            return convertProductRelatedInfo(response);
        });
    }

    /**
     * 将返回结果转换为对象
     */
    private EsProductRelatedInfo convertProductRelatedInfo(SearchResponse response) {
        EsProductRelatedInfo productRelatedInfo = new EsProductRelatedInfo();
        Map<String, Aggregation> aggregationMap = response.getAggregations().getAsMap();
        //设置品牌
        Aggregation brandNames = aggregationMap.get("brandNames");
        List<String> brandNameList = new ArrayList<>();
        for (int i = 0; i < ((Terms) brandNames).getBuckets().size(); i++) {
            brandNameList.add(((Terms) brandNames).getBuckets().get(i).getKeyAsString());
        }
        productRelatedInfo.setBrandNames(brandNameList);
        //设置分类
        Aggregation productCategoryNames = aggregationMap.get("productCategoryNames");
        List<String> productCategoryNameList = new ArrayList<>();
        for (int i = 0; i < ((Terms) productCategoryNames).getBuckets().size(); i++) {
            productCategoryNameList.add(((Terms) productCategoryNames).getBuckets().get(i).getKeyAsString());
        }
        productRelatedInfo.setProductCategoryNames(productCategoryNameList);
        //设置参数
        Aggregation productAttrs = aggregationMap.get("allAttrValues");
        List<LongTerms.Bucket> attrIds = ((LongTerms) ((InternalFilter) ((InternalNested) productAttrs).getProperty("productAttrs")).getProperty("attrIds")).getBuckets();
        List<EsProductRelatedInfo.ProductAttr> attrList = new ArrayList<>();
        for (Terms.Bucket attrId : attrIds) {
            EsProductRelatedInfo.ProductAttr attr = new EsProductRelatedInfo.ProductAttr();
            attr.setAttrId((Long) attrId.getKey());
            List<String> attrValueList = new ArrayList<>();
            List<StringTerms.Bucket> attrValues = ((StringTerms) attrId.getAggregations().get("attrValues")).getBuckets();
            List<StringTerms.Bucket> attrNames = ((StringTerms) attrId.getAggregations().get("attrNames")).getBuckets();
            for (Terms.Bucket attrValue : attrValues) {
                attrValueList.add(attrValue.getKeyAsString());
            }
            attr.setAttrValues(attrValueList);
            if (!CollectionUtils.isEmpty(attrNames)) {
                String attrName = attrNames.get(0).getKeyAsString();
                attr.setAttrName(attrName);
            }
            attrList.add(attr);
        }
        productRelatedInfo.setProductAttrs(attrList);
        return productRelatedInfo;
    }
}
