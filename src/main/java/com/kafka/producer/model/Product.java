package com.kafka.producer.model;

import com.kafka.producer.enums.Color;
import com.kafka.producer.enums.DesignType;
import com.kafka.producer.enums.ProductType;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Product {

    private Color color;

    private ProductType type;

    private DesignType designType;

}
