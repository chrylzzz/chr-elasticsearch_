package com.chryl.dao;

import com.chryl.domain.EsProduct;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 搜索系统中的商品管理自定义Dao
 * Created by Chr.yl on 2018/6/19.
 */
public interface EsProductDao {

    List<EsProduct> getAllEsProductList(@Param("id") Long id);


}
