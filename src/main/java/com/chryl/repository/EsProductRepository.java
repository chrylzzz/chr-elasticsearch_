package com.chryl.repository;

import com.chryl.domain.EsProduct;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

/**
 * 商品ES操作类
 * 继承ElasticsearchRepository接口，这样就拥有了一些基本的Elasticsearch数据操作方法，同时定义了一个衍生查询方法。
 * Created by Chr.yl on 2018/6/19.
 */
//                                                                  1:javaBean;2:idType
public interface EsProductRepository extends ElasticsearchRepository<EsProduct, Long> {

    /**
     * 搜索查询:通过商品名,标题,关键字
     *
     * @param name     商品名称
     * @param subTitle 商品标题
     * @param keywords 商品关键字
     * @param page     分页信息
     * @return
     */
    Page<EsProduct> findByNameOrSubTitleOrKeywords(String name, String subTitle, String keywords, Pageable page);

//    Page<EsProduct> findByName


}
