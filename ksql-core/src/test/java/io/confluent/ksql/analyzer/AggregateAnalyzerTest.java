/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.analyzer;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.rewrite.AggregateExpressionRewriter;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlTestUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregateAnalyzerTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();
  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = KsqlTestUtil.getNewMetaStore();
  }

  private Analysis analyze(final String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
//    System.out.println(SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " "));
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null, null));
    return analysis;
  }

  private AggregateAnalysis analyzeAggregates(final String queryStr) {
    System.out.println("Test query:" + queryStr);
    Analysis analysis = analyze(queryStr);
    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, metaStore, analysis);
    AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter();
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null, null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.getNonAggResultColumns().add(expression);
      }
      aggregateAnalysis.getFinalSelectExpressions().add(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, expression));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }

    if (analysis.getHavingExpression() != null) {
      aggregateAnalyzer.process(analysis.getHavingExpression(), new AnalysisContext(null, null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.getNonAggResultColumns().add(analysis.getHavingExpression());
      }
      aggregateAnalysis.setHavingExpression(ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter,
                                                                               analysis.getHavingExpression()));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }

    return aggregateAnalysis;
  }

  @Test
  public void testSimpleAggregateQueryAnalysis() throws Exception {
    String queryStr = "SELECT col1, count(col1) FROM test1 WHERE col0 > 100 GROUP BY col1;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);

    assertThat(aggregateAnalysis).isNotNull();
    assertThat(aggregateAnalysis.getFunctionList())
      .extracting(Object::toString)
      .containsExactly("COUNT(TEST1.COL1)");
    assertThat(aggregateAnalysis.getAggregateFunctionArguments())
      .extracting(Object::toString)
      .containsExactly("TEST1.COL1");
    assertThat(aggregateAnalysis.getRequiredColumnsList())
      .extracting(Object::toString)
      .containsExactly("TEST1.COL1");
    assertThat(aggregateAnalysis.getFinalSelectExpressions())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL1",
        "KSQL_AGG_VARIABLE_0");
    assertThat(aggregateAnalysis.getHavingExpression())
      .isNull();
  }

  @Test
  public void testMultipleAggregateQueryAnalysis() throws Exception {
    String queryStr = "SELECT col1, sum(col3), count(col1) FROM test1 WHERE col0 > 100 GROUP BY col1;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    assertThat(aggregateAnalysis.getFunctionList())
      .extracting(Object::toString)
      .containsExactly(
        "SUM(TEST1.COL3)",
        "COUNT(TEST1.COL1)");
    assertThat(aggregateAnalysis.getNonAggResultColumns())
      .extracting(Object::toString)
      .containsExactly("TEST1.COL1");
    assertThat(aggregateAnalysis.getAggregateFunctionArguments())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL3",
        "TEST1.COL1");
    assertThat(aggregateAnalysis.getFinalSelectExpressions())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL1",
        "KSQL_AGG_VARIABLE_0",
        "KSQL_AGG_VARIABLE_1");
    assertThat(aggregateAnalysis.getRequiredColumnsList())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL1",
        "TEST1.COL3");
    assertThat(aggregateAnalysis.getHavingExpression())
      .isNull();
  }

  @Test
  public void testExpressionArgAggregateQueryAnalysis() {
    String queryStr = "SELECT col1, sum(col3*col0), sum(floor(col3)*3.0) FROM test1 window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > 100 "
                      + "GROUP BY col1;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    assertThat(aggregateAnalysis.getFunctionList())
      .extracting(Object::toString)
      .containsExactly(
        "SUM((TEST1.COL3 * TEST1.COL0))",
        "SUM((FLOOR(TEST1.COL3) * 3.0))");
    assertThat(aggregateAnalysis.getAggregateFunctionArguments())
      .extracting(Object::toString)
      .containsExactly(
        "(TEST1.COL3 * TEST1.COL0)",
        "(FLOOR(TEST1.COL3) * 3.0)");
    assertThat(aggregateAnalysis.getNonAggResultColumns())
      .extracting(Object::toString)
      .containsExactly("TEST1.COL1");
    assertThat(aggregateAnalysis.getFinalSelectExpressions())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL1",
        "KSQL_AGG_VARIABLE_0",
        "KSQL_AGG_VARIABLE_1");
    assertThat(aggregateAnalysis.getRequiredColumnsList())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL1",
        "TEST1.COL3",
        "TEST1.COL0");
    assertThat(aggregateAnalysis.getHavingExpression())
      .isNull();
  }

  @Test
  public void testAggregateWithExpressionQueryAnalysis() {
    String queryStr = "SELECT col1, sum(col3*col0)/count(col1), sum(floor(col3)*3.0) FROM test1 "
                      + "window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > 100 "
                      + "GROUP BY col1;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    assertThat(aggregateAnalysis.getFunctionList())
      .extracting(Object::toString)
      .containsExactly(
        "SUM((TEST1.COL3 * TEST1.COL0))",
        "COUNT(TEST1.COL1)",
        "SUM((FLOOR(TEST1.COL3) * 3.0))");
    assertThat(aggregateAnalysis.getAggregateFunctionArguments())
      .extracting(Object::toString)
      .containsExactly(
        "(TEST1.COL3 * TEST1.COL0)",
        "TEST1.COL1",
        "(FLOOR(TEST1.COL3) * 3.0)");
    assertThat(aggregateAnalysis.getNonAggResultColumns())
      .extracting(Object::toString)
      .containsExactly("TEST1.COL1");
    assertThat(aggregateAnalysis.getFinalSelectExpressions())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL1",
        "(KSQL_AGG_VARIABLE_0 / KSQL_AGG_VARIABLE_1)",
        "KSQL_AGG_VARIABLE_2");
    assertThat(aggregateAnalysis.getRequiredColumnsList())
      .extracting(Object::toString)
      .containsExactly("TEST1.COL1", "TEST1.COL3", "TEST1.COL0");
    assertThat(aggregateAnalysis.getHavingExpression())
      .isNull();
  }

  @Test
  public void testAggregateWithExpressionHavingQueryAnalysis() {
    String queryStr = "SELECT col1, sum(col3*col0)/count(col1), sum(floor(col3)*3.0) FROM test1 "
                      + "window w "
                      + "TUMBLING ( size 2 second) WHERE col0 > 100 "
                      + "GROUP BY col1 "
                      + "HAVING count(col1) > 10;";
    AggregateAnalysis aggregateAnalysis = analyzeAggregates(queryStr);
    assertThat(aggregateAnalysis.getFunctionList())
      .extracting(Object::toString)
      .containsExactly(
        "SUM((TEST1.COL3 * TEST1.COL0))",
        "COUNT(TEST1.COL1)",
        "SUM((FLOOR(TEST1.COL3) * 3.0))",
        "COUNT(TEST1.COL1)");
   assertThat(aggregateAnalysis.getAggregateFunctionArguments())
      .extracting(Object::toString)
      .containsExactly(
        "(TEST1.COL3 * TEST1.COL0)",
        "TEST1.COL1",
        "(FLOOR(TEST1.COL3) * 3.0)",
        "TEST1.COL1");
   assertThat(aggregateAnalysis.getNonAggResultColumns())
      .extracting(Object::toString)
      .containsExactly("TEST1.COL1");
   assertThat(aggregateAnalysis.getFinalSelectExpressions())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL1",
        "(KSQL_AGG_VARIABLE_0 / KSQL_AGG_VARIABLE_1)",
        "KSQL_AGG_VARIABLE_2");
   assertThat(aggregateAnalysis.getRequiredColumnsList())
      .extracting(Object::toString)
      .containsExactly(
        "TEST1.COL1",
        "TEST1.COL3",
        "TEST1.COL0");
    assertThat(aggregateAnalysis.getHavingExpression())
      .isInstanceOf(ComparisonExpression.class)
      .extracting(Object::toString).containsExactly("(KSQL_AGG_VARIABLE_3 > 10)");
  }
}
