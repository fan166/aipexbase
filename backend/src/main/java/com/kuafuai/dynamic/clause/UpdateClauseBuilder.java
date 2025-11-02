package com.kuafuai.dynamic.clause;

import com.kuafuai.common.exception.BusinessException;
import com.kuafuai.common.util.StringUtils;
import com.kuafuai.dynamic.context.TableContext;
import com.kuafuai.system.entity.AppTableColumnInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateClauseBuilder {
    private final TableContext ctx;

    public UpdateClauseBuilder(TableContext ctx) {
        this.ctx = ctx;
    }

    public String build() {
        Map<String, Object> cond = ctx.getConditions();

        AppTableColumnInfo pk = ctx.getColumns().stream()
                .filter(AppTableColumnInfo::isPrimary)
                .findFirst()
                .orElseThrow(() -> new BusinessException("dynamic.table.not_primary", ctx.getTable()));

        if (!cond.containsKey(pk.getColumnName())) {
            throw new BusinessException("dynamic.update.params.primary", ctx.getTable());
        }

        List<String> sets = new ArrayList<>();
        for (AppTableColumnInfo c : ctx.getColumns()) {
            if (c.isPrimary())
                continue;
            String k = c.getColumnName();
            Object v = cond.get(k);
            if (v == null)
                continue;
            if (v instanceof Map)
                continue;
            String str = String.valueOf(v);
            if (StringUtils.isNotNull(str)) {
                sets.add("`" + k + "` = #{conditions." + k + "}");
            }
        }

        if (sets.isEmpty())
            return "SELECT 1";

        Object pkValue = cond.get(pk.getColumnName());
        if (pkValue instanceof Map) {
            return "UPDATE " + ctx.qualifiedTable() + " SET " + String.join(", ", sets)
                    + " WHERE `" + pk.getColumnName() + "` = #{conditions." + pk.getColumnName() + ".eq}";
        } else {
            return "UPDATE " + ctx.qualifiedTable() + " SET " + String.join(", ", sets)
                    + " WHERE `" + pk.getColumnName() + "` = #{conditions." + pk.getColumnName() + "}";
        }
    }
}
