import java

predicate isFixtureAnnotation(AnnotationType t) {
  t.getName().matches("Before%") or
  t.getName().matches("After%")
}

predicate reliesOnFixturesP(Class c) {
  exists(AnnotationType t |
    t = c.getAnAnnotation().getType() and
    isFixtureAnnotation(t)
  )
}

boolean reliesOnFixtures(Method m) {
  if reliesOnFixturesP(m.getDeclaringType()) then
    result = true
  else
    result = false
}

predicate hasKnownPackage(Callable c) {
  exists(string n |
    n = c.getDeclaringType().getPackage().getName() and
    (
      // Java Builtin
      n.matches("java.lang%") or
      n.matches("java.util%") or
      n.matches("java.io%") or
      n.matches("java.net%") or
      n.matches("java.math%") or
      n.matches("java.time%") or
      // JSON Libraries
      n.matches("org.json%") or
      n.matches("com.google.code.json") or
      // Logging Libraries
      n.matches("org.apache.logging%") or
      n.matches("org.slf4j") or
      // Common Libraries
      n.matches("com.fasterxml.jackson") or
      n.matches("gnu.trove") or
      n.matches("org.apache.commons%") or
      n.matches("com.google.guava%") or
      n.matches("org.joda.time%") or
      n.matches("org.eclipse%") or
      n.matches("com.google.collect%") or

      // Testing + Assertion Libraries
      n.matches("org.junit%") or
      n.matches("org.testng%") or
      n.matches("org.assertj%") or
      n.matches("com.google.truth") or
      n.matches("org.hamcrest") or
      n.matches("org.valid4j") or
      n.matches("net.ttddyy")
    )
  )
}

predicate contains(Method target, MethodCall call){
    call.getCaller() = target or call.getCaller().getEnclosingCallable() = target
}

int getClassInvocations(Method testMethod){
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
where t.fromSource() and
      t.getDeclaringType().hasName("{{ class_name }}") and
      t.hasName("{{ method_name }}") and
      t.getFile() = targetFile
select count(FieldAccess f | f.getEnclosingCallable() = t and f.getField().fromSource() | f) as field_accesses,
  reliesOnFixtures(t) as relies_fixtures,
  getNonClassUnknownInvocations(t) as diff_class_unknown_invoc,
  count(ConditionalStmt cst | cst.getEnclosingCallable() = t | cst) as branch_count,
  numSpecialFeatures(t) as special_count,
  t.getMetrics().getHalsteadLength() as halstead_length,
  t.getMetrics().getCyclomaticComplexity() as cyclomatic_complexity,
  t.getMetrics().getEfferentCoupling() as efferent_coupling,
  t.getMetrics().getAfferentCoupling() as afferent_coupling,
  t.getDeclaringType().getMetrics().getMaintainabilityIndex() as maintainability_index,
  t.getStringSignature() as gsig