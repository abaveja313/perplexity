import java

predicate hasKnownPackage(Callable c) {
  c.getDeclaringType().getPackage().getName() in [
    // Java Builtin
    "java.lang%", "java.util%", "java.io%", "java.net%", "java.math%", "java.time%",
    // JSON Libraries
    "org.json%", "com.google.code.json",
    // Logging Libraries 
    "org.apache.logging%", "org.slf4j",
    // Common Libraries
    "com.fasterxml.jackson", "gnu.trove", "org.apache.commons%", "com.google.guava%", "org.joda.time%", "org.eclipse%"
  ]
}

predicate contains(Method target, MethodCall call) {
  call.getCaller() = target
  or
  call.getCaller().getEnclosingCallable() = target
}

int getClassOtherInvocations(Method targetMethod) {
  result = count(MethodCall mCall |
    contains(targetMethod, mCall) and
    mCall.getCallee().getDeclaringType() = targetMethod.getDeclaringType()
  )
}

int getNonClassUnknownInvocations(Method targetMethod) {
  result = count(MethodCall mCall |
    contains(targetMethod, mCall) and
    mCall.getCallee().getDeclaringType() != targetMethod.getDeclaringType() and
    not hasKnownPackage(mCall.getCallee())
  )
}

int getNonClassKnownInvocations(Method targetMethod) {
  result = count(MethodCall mCall |
    contains(targetMethod, mCall) and
    mCall.getCallee().getDeclaringType() != targetMethod.getDeclaringType() and
    hasKnownPackage(mCall.getCallee())
  )
}

int numLambdaAndFunctionalExpr(Method m) {
  result = count(Expr e |
    e.getEnclosingCallable() = m and
    (e instanceof LambdaExpr or e instanceof FunctionalExpr)
  )
}

int numSpecialMethodAccess(Method m) {
  result = count(Expr e |
    e.getEnclosingCallable() = m and
    (e instanceof MemberRefExpr or e instanceof PropertyRefExpr)
  )
}

int numSpecialControlFlow(Method m) {
  result = count(Expr e |
    e.getEnclosingCallable() = m and
    (e instanceof SwitchExpr or e instanceof UnsafeCoerceExpr)
  )
}

int numSpecialLiterals(Method m) {
  result = count(Expr e |
    e.getEnclosingCallable() = m and
    (e instanceof StringTemplateExpr or e instanceof CharacterLiteral)
  )
}

int numSpecialIncDec(Method m) {
  result = count(Expr e |
    e.getEnclosingCallable() = m and
    (e instanceof PreIncExpr or e instanceof PreDecExpr)
  )
}

int numSpecialPatterns(Method m) {
  result = count(RecordPatternExpr e | e.getEnclosingCallable() = m)
}

class TargetFile extends File {
  TargetFile() {
    this.isJavaSourceFile() and
    this.getRelativePath() = "{{ relative_path }}"
  }
}

from Method m, TargetFile targetFile
where
  m.fromSource() and
  m.getDeclaringType().hasName("{{ class_name }}") and
  m.hasName("{{ method_name }}") and
  m.getFile() = targetFile
select
  count(Field f | m.accesses(f) and f.fromSource()) as field_accesses,
  count(Field f | m.writes(f) and f.fromSource()) as field_writes,
  getClassOtherInvocations(m) as same_class_other_invoc,
  getNonClassKnownInvocations(m) as diff_class_known_invoc,
  getNonClassUnknownInvocations(m) as diff_class_unknown_invoc,
  count(ConditionalStmt cst | cst.getEnclosingCallable() = m) as branch_count,
  numLambdaAndFunctionalExpr(m) as lambda_functional_count,
  numSpecialMethodAccess(m) as special_method_access_count,
  numSpecialControlFlow(m) as special_control_flow_count,
  numSpecialLiterals(m) as special_literals_count,
  numSpecialIncDec(m) as special_incdec_count,
  numSpecialPatterns(m) as special_patterns_count,
  m.getMetrics().getHalsteadLength() as halstead_length,
  m.getMetrics().getHalsteadVocabulary() as halstead_vocab,
  m.getMetrics().getCyclomaticComplexity() as cyclomatic_complexity,
  m.getMetrics().getEfferentCoupling() as efferent_coupling,
  m.getMetrics().getAfferentCoupling() as afferent_coupling,
  m.getDeclaringType().getMetrics().getMaintainabilityIndex() as maintainability_index,
  m.getDeclaringType().getMetrics().getMaintainabilityIndexWithoutComments() as maintainability_index_no_comments,
  m.getMetrics().getNumberOfParameters() as num_params,
  m.getMetrics().getNumberOfLinesOfCode() as num_lines_code,
  m.getStringSignature() as gsig
