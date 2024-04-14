import java

predicate isFixtureAnnotation(AnnotationType t) {
  t.getName().regexpMatch("(Before|After).*")
}

predicate reliesOnFixturesP(Class c) {
  c.getAnAnnotation().getType() instanceof FixtureAnnotation
}

boolean reliesOnFixtures(Method m) {
  reliesOnFixturesP(m.getDeclaringType())
}

predicate hasKnownPackage(Callable c) {
  c.getDeclaringType().getPackage().getName() in [
    // Java Builtin
    "java.lang%", "java.util%", "java.io%", "java.net%", "java.math%", "java.time%",
    // JSON Libraries
    "org.json%", "com.google.code.json",
    // Logging Libraries
    "org.apache.logging%", "org.slf4j",
    // Common Libraries
    "com.fasterxml.jackson", "gnu.trove", "org.apache.commons%", "com.google.guava%", "org.joda.time%", "org.eclipse%", "com.google.collect%",
    // Testing + Assertion Libraries
    "org.junit%", "org.testng%", "org.assertj%", "com.google.truth", "org.hamcrest", "org.valid4j", "net.ttddyy"
  ]
}

predicate contains(Method target, MethodCall call) {
  call.getCaller() = target or call.getCaller().getEnclosingCallable() = target
}

int getClassInvocations(Method testMethod) {
  result = count(MethodCall mCall |
    contains(testMethod, mCall) and
    mCall.getCaller().getDeclaringType() = testMethod.getDeclaringType()
  )
}

int getNonClassUnknownInvocations(Method testMethod) {
  result = count(MethodCall mCall |
    contains(testMethod, mCall) and
    mCall.getCallee().getDeclaringType().getPackage() != testMethod.getDeclaringType().getPackage() and
    not hasKnownPackage(mCall.getCallee())
  )
}

int numSpecialFeatures(Method m) {
  result = count(Expr e |
    e.getEnclosingCallable() = m and
    (
      e instanceof LambdaExpr or
      e instanceof VirtualMethodAccess or
      e instanceof FunctionalExpr or
      e instanceof SwitchExpr or
      e instanceof StringTemplateExpr or
      e instanceof MemberRefExpr or
      e instanceof PropertyRefExpr or
      e instanceof UnsafeCoerceExpr or
      e instanceof RecordPatternExpr or
      e instanceof CharacterLiteral or
      e instanceof PreIncExpr or
      e instanceof PostIncExpr
    )
  )
}

class TargetFile extends File {
  TargetFile() {
    this.isJavaSourceFile() and
    this.getRelativePath() = "{{ relative_path }}"
  }
}

from Method t, TargetFile targetFile
where
  t.fromSource() and
  t.getDeclaringType().hasName("{{ class_name }}") and
  t.hasName("{{ method_name }}") and
  t.getFile() = targetFile
select
  count(FieldAccess f | f.getEnclosingCallable() = t and f.getField().fromSource()) as field_accesses,
  reliesOnFixtures(t) as relies_fixtures,
  getNonClassUnknownInvocations(t) as diff_class_unknown_invoc,
  count(ConditionalStmt cst | cst.getEnclosingCallable() = t) as branch_count,
  numSpecialFeatures(t) as special_count,
  t.getMetrics().getHalsteadLength() as halstead_length,
  t.getMetrics().getCyclomaticComplexity() as cyclomatic_complexity,
  t.getMetrics().getEfferentCoupling() as efferent_coupling,
  t.getMetrics().getAfferentCoupling() as afferent_coupling,
  t.getDeclaringType().getMetrics().getMaintainabilityIndex() as maintainability_index
