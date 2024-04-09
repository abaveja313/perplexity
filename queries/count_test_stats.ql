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

predicate hasKnownPackage(Method m) {
  m.getDeclaringType().getPackage().getName().regexpMatch("(java\\.(lang|util|io|net|math|time).*|org\\.(json|apache\\.(logging|commons).*|slf4j|eclipse.*)|com\\.(google\\.(code\\.json|guava|truth)|fasterxml\\.jackson)|gnu\\.trove|org\\.(joda\\.time|junit|testng|assertj|hamcrest|valid4j)|net\\.ttddyy).*")
}

int getClassInvocations(Method testMethod){
    result = count(MethodCall mCall |
        mCall.getCallee().getDeclaringType() = testMethod.getDeclaringType()
    )
}

int getNonClassUnknownInvocations(Method testMethod) {
  result = count(MethodCall mCall |
    mCall.getCallee().getDeclaringType().getPackage() != testMethod.getDeclaringType().getPackage() and
    not hasKnownPackage(mCall.getCallee())
  )
}

int numSpecialFeatures(Method m) {
  result = count(Expr e |
    e.getEnclosingCallable() = m and
    e instanceof LambdaExpr or
    e instanceof FunctionalExpr or
    e instanceof SwitchExpr or
    e instanceof MemberRefExpr or
    e instanceof RecordPatternExpr or
    e instanceof PreIncExpr or
    e instanceof PostIncExpr
  )
}

from Method t
where
  t.fromSource() and
  t.getDeclaringType().hasName("{{ class_name }}") and t.hasStringSignature("{{ method_sig }}")
select count(FieldAccess f | f.getEnclosingCallable() = t and f.getField().fromSource() and not f.getField().isFinal() | f) as field_accesses,
  reliesOnFixtures(t) as relies_fixtures,
  getNonClassUnknownInvocations(t) as diff_class_unknown_invoc,
  count(ConditionalStmt cst | cst.getEnclosingCallable() = t | cst) as branch_count,
  numSpecialFeatures(t) as special_count,
  t.getMetrics().getHalsteadLength() as halstead_length,
  t.getMetrics().getCyclomaticComplexity() as cyclomatic_complexity,
  t.getMetrics().getEfferentCoupling() as efferent_coupling,
  t.getMetrics().getAfferentCoupling() as afferent_coupling,
  t.getDeclaringType().getMetrics().getMaintainabilityIndex() as maintainability_index