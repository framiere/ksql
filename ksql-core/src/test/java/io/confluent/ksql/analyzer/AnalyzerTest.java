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
import io.confluent.ksql.parser.rewrite.SqlFormatterQueryRewrite;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlTestUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyzerTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = KsqlTestUtil.getNewMetaStore();
  }

  private Analysis analyze(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    System.out.println(SqlFormatterQueryRewrite.formatSql(statements.get(0)).replace("\n", " "));
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null, null));
    return analysis;
  }

  @Test
  public void testSimpleQueryAnalysis() {
    String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    Analysis analysis = analyze(simpleQuery);
    assertThat(analysis.getInto())
      .isNotNull();
    assertThat(analysis.getFromDataSources())
      .isNotNull();
    assertThat(analysis.getSelectExpressions())
      .isNotNull();
    assertThat(analysis.getSelectExpressionAlias())
      .isNotNull();
    assertThat(analysis.getFromDataSources())
      .extracting(e -> e.left.getName())
      .containsExactly("TEST1");
    assertThat(analysis.getFromDataSources())
      .extracting(Object::toString)
      .containsExactly("KsqlStream name:TEST1,TEST1");
    assertThat(analysis.getSelectExpressions())
      .hasSameSizeAs(analysis.getSelectExpressionAlias())
      .as("Select expression and select expression alias must have same sizes");

    assertThat(SqlFormatterQueryRewrite.formatSql(analysis.getWhereExpression()))
      .isEqualTo("(TEST1.COL0 > 100)");
    assertThat(analysis.getSelectExpressions().stream().map(SqlFormatterQueryRewrite::formatSql).collect(Collectors.toList()))
      .containsExactly(
        "TEST1.COL0",
        "TEST1.COL2",
        "TEST1.COL3");
    assertThat(analysis.getSelectExpressionAlias())
      .extracting(Object::toString)
      .containsExactly(
        "COL0",
        "COL2",
        "COL3");
  }

  @Test
  public void testSimpleLeftJoinAnalysis() {
    String
        simpleQuery =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
        + "t1.col1 = t2.col1;";
    Analysis analysis = analyze(simpleQuery);
    assertThat(analysis.getInto())
      .isNotNull();
    assertThat(analysis.getFromDataSources())
      .isNotNull();
    assertThat(analysis.getJoin())
      .isNotNull();
    assertThat(analysis.getSelectExpressions())
      .isNotNull();
    assertThat(analysis.getSelectExpressionAlias())
      .isNotNull();
    assertThat(analysis.getJoin().getLeftAlias())
      .isEqualTo("T1");
    assertThat(analysis.getJoin().getRightAlias())
      .isEqualTo("T2");
    assertThat(analysis.getSelectExpressions())
      .hasSameSizeAs(analysis.getSelectExpressionAlias())
      .as("Select expression and select expression alias must have same sizes");


    assertThat(analysis.getJoin().getLeftKeyFieldName())
      .isEqualTo("COL1");
    assertThat(analysis.getJoin().getRightKeyFieldName())
      .isEqualTo("COL1");

    assertThat(analysis.getSelectExpressions().stream().map(SqlFormatterQueryRewrite::formatSql).collect(Collectors.toList()))
      .containsExactly(
        "T1.COL1",
        "T2.COL1",
        "T2.COL4",
        "T1.COL5",
        "T2.COL2");
    assertThat(analysis.getSelectExpressionAlias())
      .extracting(Object::toString)
      .containsExactly(
        "T1_COL1",
        "T2_COL1",
        "T2_COL4",
        "COL5",
        "T2_COL2");
  }

  @Test
  public void testBooleanExpressionAnalysis() {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    Analysis analysis = analyze(queryStr);

    assertThat(analysis.getInto())
      .isNotNull();
    assertThat(analysis.getFromDataSources())
      .isNotNull();
    assertThat(analysis.getSelectExpressions())
      .isNotNull();
    assertThat(analysis.getSelectExpressionAlias())
      .isNotNull();
    assertThat(analysis.getFromDataSources().stream().map(e -> e.left.getName()).collect(Collectors.toList()))
      .as("FROM was not analyzed correctly.")
      .containsExactly("TEST1");

    assertThat(analysis.getSelectExpressions())
      .extracting(Object::toString)
      .containsExactly(
        "(TEST1.COL0 = 10)",
        "TEST1.COL2",
        "(TEST1.COL3 > TEST1.COL1)");
  }

  @Test
  public void testFilterAnalysis() {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1 WHERE col0 > 20;";
    Analysis analysis = analyze(queryStr);

    assertThat(analysis.getInto())
      .isNotNull();
    assertThat(analysis.getFromDataSources())
      .isNotNull();
    assertThat(analysis.getSelectExpressions())
      .isNotNull();
    assertThat(analysis.getSelectExpressionAlias())
      .isNotNull();
    assertThat(analysis.getFromDataSources().stream().map(e -> e.left.getName()).collect(Collectors.toList()))
      .as("FROM was not analyzed correctly.")
      .containsExactly("TEST1");

    assertThat(analysis.getSelectExpressions())
      .extracting(Object::toString)
      .containsExactly(
        "(TEST1.COL0 = 10)",
        "TEST1.COL2",
        "(TEST1.COL3 > TEST1.COL1)");

    assertThat(analysis.getWhereExpression())
      .extracting(Object::toString)
      .containsExactly("(TEST1.COL0 > 20)");
  }
}
