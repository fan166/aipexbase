package com.kuafuai.dynamic.validation;

import com.kuafuai.common.exception.BusinessException;
import com.kuafuai.common.util.I18nUtils;
import com.kuafuai.system.entity.AppTableColumnInfo;

import java.util.List;
import java.util.Map;

public class RequiredColumnChecker {

    public static void check(List<AppTableColumnInfo> columns, Map<String, Object> conditions) {
        StringBuilder sb = new StringBuilder();

        for (AppTableColumnInfo columnInfo : columns) {
            if (!columnInfo.isPrimary() && !columnInfo.isNullable()) {
                String col = columnInfo.getColumnName();
                if (!conditions.containsKey(col) || conditions.get(col) == null) {
                    sb.append(I18nUtils.get("dynamic.table.column.not_data", col)).append("\n");
                }
            }
        }

        if (sb.length() > 0) {
            throw new BusinessException(sb.toString());
        }
    }
}
