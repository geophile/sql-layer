/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2009-2015 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.foundationdb.server.types.service;

import com.foundationdb.server.error.ArgumentTypeRequiredException;
import com.foundationdb.server.error.NoCommonTypeException;
import com.foundationdb.server.error.NoSuchCastException;
import com.foundationdb.server.error.NoSuchFunctionOverloadException;
import com.foundationdb.server.error.NoSuchFunctionException;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.types.InputSetFlags;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TInstanceAdjuster;
import com.foundationdb.server.types.TInstanceBuilder;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.texpressions.TValidatedOverload;
import com.foundationdb.server.types.mcompat.mtypes.MString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class OverloadResolver<V extends TValidatedOverload> {

    public static class OverloadResult<V extends TValidatedOverload> {
        private V overload;
        /** Could be have null values, or be null itself */
        private TInstance[] instances;
        /**
         * null if there is no pickingSet in the overload, or if one of `instances` is null.
         */
        private TInstance pickedInstance;

        private class AdjusterImpl implements TInstanceAdjuster {
            private TInstanceBuilder[] builders;

            void setInputSet(TInputSet inputSet) {
                this.inputSet = inputSet;
            }

            void copyBack() {
                if (builders != null) {
                    for (int i = 0; i < builders.length; ++i) {
                        TInstanceBuilder builder = builders[i];
                        if (builder != null)
                            instances[i] = builder.get();
                    }
                }
            }

            @Override
            public TInstance get(int i) {
                check(i);
                if (builders != null && builders[i] != null)
                    instances[i] = builders[i].get(); // update the TInstance if needed
                return instances[i];
            }

            @Override
            public TInstanceBuilder adjust(int i) {
                check(i);
                if (builders == null)
                    builders = new TInstanceBuilder[nInputs];
                TInstanceBuilder result = builders[i];
                if (result == null) {
                    result = new TInstanceBuilder(instances[i]);
                    builders[i] = result;
                    instances[i] = null;
                }
                return result;
            }

            @Override
            public void replace(int i, TInstance type) {
                if (builders != null && builders[i] != null)
                    builders[i].copyFrom(type);
                else
                    instances[i] = type;
            }

            void check(int i) {
                assert overload.inputSetAt(i) == inputSet
                        : "input set at " + i + " is " + overload.inputSetAt(i) + ", expected " + inputSet;
            }

            private AdjusterImpl(TValidatedOverload overload, int nInputs) {
                this.overload = overload;
                this.nInputs = nInputs;
            }

            private final int nInputs;
            private final TValidatedOverload overload;
            private TInputSet inputSet;
        }

        private OverloadResult(V overload, List<? extends TPreptimeValue> inputs, TCastResolver resolver)
        {
            this.overload = overload;
            List<TInputSet> inputSets = overload.inputSets();
            int nInputs = inputs.size();
            AdjusterImpl adjuster = null;
            for (TInputSet inputSet : inputSets) {
                TClass targetTClass = inputSet.targetType();
                if (targetTClass == null) {
                    TInstance type = findCommon(overload, inputSet, inputs, resolver);
                    fillInstances(overload, inputSet, type, nInputs);
                }
                else {
                    if (adjuster == null)
                        adjuster = new AdjusterImpl(overload, nInputs);
                    adjuster.setInputSet(inputSet);
                    findInstance(overload, inputSet, inputs, resolver);
                    inputSet.instanceAdjuster().apply(adjuster, overload, inputSet, nInputs);
                    adjuster.copyBack();
                }
            }
            pickedInstance = findPickedInstance(overload, inputs);
        }

        private TInstance findPickedInstance(V overload, List<? extends TPreptimeValue> inputs) {
            TInputSet pickingSet = overload.pickingInputSet();
            TInstance result = null;
            if (pickingSet != null) {
                for (int i = overload.firstInput(pickingSet), max = inputs.size();
                     i >= 0;
                     i = overload.nextInput(pickingSet, i+1, max))
                {
                    TInstance inputInstance = instances[i];
                    // if we need to pickInstance, we'll do it on the previous result's TClass. That covers the case
                    // of a picking ANY, in which case findCommon would have found a TClass.
                    result = (result == null)
                            ? inputInstance
                            : result.typeClass().pickInstance(result, inputInstance);
                }
            }
            return result;
        }

        private void fillInstances(V func, TInputSet inputSet, TInstance type, int inputsLen) {
            if (instances == null)
                instances = new TInstance[inputsLen];
            else assert inputsLen == instances.length
                    : inputsLen + "not size of " + Arrays.toString(instances);
            for (int i = func.firstInput(inputSet); i >= 0; i = func.nextInput(inputSet, i+1, inputsLen)) {
                instances[i] = type;
            }
        }

        private void findInstance(V overload, TInputSet inputSet,
                                       List<? extends TPreptimeValue> inputs,
                                       TCastResolver resolver)
        {
            final TClass targetTClass = inputSet.targetType();
            assert targetTClass != null;

            int lastPositionalInput = overload.positionalInputs();
            boolean notVararg = ! overload.isVararg();
            for (int i = 0, size = inputs.size(); i < size; ++i) {
                if (overload.inputSetAt(i) != inputSet)
                    continue;
                if (notVararg && (i >= lastPositionalInput))
                    break;
                TPreptimeValue inputTpv = inputs.get(i);
                TInstance inputInstance = inputTpv.type();
                TInstance resultInstance;
                if (inputInstance != null) {
                    TClass inputTClass = inputInstance.typeClass();
                    if (inputTClass == targetTClass) {
                        resultInstance = inputInstance;
                    }
                    else {
                        TCast requiredCast = resolver.cast(inputTClass, targetTClass);
                        if (requiredCast == null)
                            throw new NoSuchCastException(inputInstance.typeClass(), targetTClass);
                        inputInstance = requiredCast.preferredTarget(inputTpv);
                        resultInstance = inputInstance;
                    }
                }
                // no inputInstance = no type attributes
                else {
                    assert inputTpv.isNullable() : inputTpv;
                    // TODO: Generalize to e.g. instance(nullable) -> unknownInstance(nullable) ?
                    if(targetTClass instanceof TString) {
                        resultInstance = targetTClass.instance(Integer.MAX_VALUE, // no length would be preferable
                                                               StringFactory.DEFAULT_CHARSET_ID,
                                                               StringFactory.NULL_COLLATION_ID,
                                                               true);
                    } else {
                        resultInstance = targetTClass.instance(true);
                    }
                }
                if (instances == null)
                    instances = new TInstance[size];
                instances[i] = resultInstance;
            }
        }

        /**
         * Never returns null.
         * Note: If !inputSet.isPicking() and no common tclass can be found, then VARCHAR(0) will be returned.
         */
        private TInstance findCommon(V overload, TInputSet inputSet,
                                  List<? extends TPreptimeValue> inputs, TCastResolver resolver)
        {
            assert inputSet.targetType() == null : inputSet; // so we have to look at inputs
            TClass common = null;
            TInstance commonInst = null;
            int lastPositionalInput = overload.positionalInputs();
            boolean notVararg = ! overload.isVararg();
            boolean nullable = false;
            for (int i = 0, size = inputs.size(); i < size; ++i) {
                if (overload.inputSetAt(i) != inputSet)
                    continue;
                if (notVararg && (i >= lastPositionalInput))
                    break;
                TPreptimeValue inputTpv = inputs.get(i);
                nullable |= inputTpv.isNullable();
                TInstance inputInstance = inputTpv.type();
                if (inputInstance == null) {
                    // unknown type, like a NULL literal or parameter
                    continue;
                }
                TClass inputClass = inputInstance.typeClass();
                if (common == null) {
                    // First input we've seen, so just use it.
                    common = inputClass;
                    commonInst = inputInstance;
                }
                else if (inputClass == common) {
                    // saw the same TClass as before, so pick it
                    commonInst = (commonInst == null) ? inputInstance : common.pickInstance(commonInst, inputInstance);
                }
                else {
                    // Saw a different TCLass as before, so need to cast one of them. We'll get the new common type,
                    // at which point we have exactly one of three possibilities:
                    //   1) newCommon == [old] common, in which case we'll keep the old TInstance
                    //   2) newCommon == inputClass, in which case we'll use the inputInstance
                    //   3) newCommon is neither, in which case we'll generate a new TInstance
                    // We know that we can't have both #1 and #2, because that would imply [old] common == inputClass,
                    // which has already been handled.
                    TClass newCommon = resolver.commonTClass(common, inputClass);
                    if (newCommon == null)
                        throw new NoCommonTypeException(overload.displayName(), typeNameList(inputs));

                    if (newCommon == inputClass) { // case #2
                        common = newCommon;
                        commonInst = inputInstance;
                    }
                    else if (newCommon != common) { // case #3
                        common = newCommon;
                        commonInst = null;
                    }
                    // else if (newCommon = common), we don't need this because there's nothing to do in this case
                }
            }
            if (common == null) {
                if (!inputSet.isPicking())
                    return MString.VARCHAR.instance(0, nullable); // Unknown type and callee doesn't care.
                else
                    throw new ArgumentTypeRequiredException(overload.displayName(), inputSet);
            }
            return (commonInst == null)
                ? common.instance(nullable)
                : commonInst;
        }

        public V getOverload() {
            return overload;
        }

        public TInstance getPickedInstance() {
            if (pickedInstance == null)
                throw new IllegalStateException("no picked instance");
            return pickedInstance;
        }

        public TInstance getTypeClass(int inputIndex) {
            return instances[inputIndex];
        }
    }

    private final ResolvablesRegistry<V> overloadsRegistry;
    private final TCastResolver castsResolver;

    public OverloadResolver(ResolvablesRegistry<V> overloadsRegistry, TCastResolver castsResolver) {
        this.overloadsRegistry = overloadsRegistry;
        this.castsResolver = castsResolver;
    }

    public boolean isDefined(String name) {
        return overloadsRegistry.containsKey(name);
    }

    public OverloadResult<V> get(String name, List<? extends TPreptimeValue> inputs)
    {
        Iterable<? extends ScalarsGroup<V>> scalarsGroup = overloadsRegistry.get(name);
        if (scalarsGroup == null) {
            throw new NoSuchFunctionException(name);
        }
        return inputBasedResolution(name, inputs, scalarsGroup);
    }

    private OverloadResult<V> inputBasedResolution(
            String name, List<? extends TPreptimeValue> inputs,
            Iterable<? extends ScalarsGroup<V>> scalarGroupsByPriority)
    {
        V mostSpecific = null;
        boolean sawRightArity = false;
        int aritySeen = -1;
        
        Iterator<? extends ScalarsGroup<V>> iter = scalarGroupsByPriority.iterator();
        ScalarsGroup<V> scalarsGroup;
        
        while (iter.hasNext()) {
            scalarsGroup = iter.next();
            Collection<? extends V> namedOverloads = scalarsGroup.getOverloads();
            List<V> candidates = new ArrayList<>(namedOverloads.size());
            for (V overload : namedOverloads) {
                if (!overload.coversNInputs(inputs.size())) {
                    aritySeen = overload.positionalInputs();
                    continue;
                }
                sawRightArity = true;
                if (isCandidate(overload, inputs, scalarsGroup, iter.hasNext())) {
                    candidates.add(overload);
                }
            }
            if (candidates.isEmpty())
                continue; // try next priority group of namedOverloads
            if (candidates.size() == 1) {
                mostSpecific = candidates.get(0);
                break; // found one!
            } else {
                List<List<V>> groups = reduceToMinimalCastGroups(candidates);
                if (groups.size() == 1 && groups.get(0).size() == 1) {
                    mostSpecific = groups.get(0).get(0);
                    break; // found one!
                }
                else {
                    // Too many candidates in priority group.
                    // An error that is normally caught by function registry.
                    throw new IllegalStateException(name + " has too many candidates for " + typeNameList(inputs));
                }
            }
        }
        if (mostSpecific == null) {
            // no priority group had any candidates; this is an error
            if (sawRightArity)
                throw new NoSuchFunctionOverloadException(name, typeNameList(inputs));
            throw new WrongExpressionArityException(aritySeen, inputs.size());
        }
        return buildResult(mostSpecific, inputs);
    }

    ResolvablesRegistry<V> getRegistry() {
        return overloadsRegistry;
    }

    private static String typeNameList(List<? extends TPreptimeValue> inputs) {
        StringBuilder sb = new StringBuilder();
        for(TPreptimeValue tpv : inputs) {
            if(sb.length() > 0) {
                sb.append(",");
            }
            if(tpv == null || tpv.type() == null) {
                sb.append("?");
            } else {
                sb.append(tpv.type().typeClass().name().unqualifiedName());
            }
        }
        return sb.toString();
    }

    private boolean isCandidate(V overload,
                                List<? extends TPreptimeValue> inputs,
                                ScalarsGroup<V> scalarGroups,
                                boolean hasNext) {
        if (!overload.coversNInputs(inputs.size()))
            return false;

        InputSetFlags exactInputs = overload.exactInputs();
        TClass[] pickSameType = null;
        for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
            // allow this input if
            // all overloads of this name have the same at this position
            boolean requireExact = exactInputs.get(i);
            if ( (!requireExact) && scalarGroups.hasSameTypeAt(i)) {
                continue;
            }

            TPreptimeValue inputTpv = inputs.get(i);
            TInstance inputInstance = (inputTpv == null) ? null : inputTpv.type();
            // allow this input if...
            // ... input set takes ANY, and it isn't marked as an exact. If it's marked as an exact, we'll figure it
            // out later
            TInputSet inputSet = overload.inputSetAt(i);
            if ((!requireExact) && inputSet.targetType() == null) {
                continue;
            }
            // ... input can be strongly cast to input set
            TClass inputTypeClass;
            if (requireExact) {
                inputTypeClass = (inputInstance == null) ? null : inputInstance.typeClass();
            }
            else if (inputInstance == null) {
                // If input type is unknown (NULL literal or parameter), assume common type at this position among
                // all overloads in this group.
                inputTypeClass = scalarGroups.commonTypeAt(i);
                if (inputTypeClass == null) { // We couldn't resolve it in this group
                    if (hasNext)              // , but we might find a match in the subsequent ones
                        return false;
                    else
                        throw new ArgumentTypeRequiredException(overload.displayName(), i);
                }
            }
            else {
                inputTypeClass = inputInstance.typeClass();
            }

            if (requireExact) {
                if (inputSet.targetType() == null) {
                    // We're in an ANY-exact input set. The semantics are:
                    // - unknown types are always allowed
                    // - the first known type defines the type of the input set
                    // - subsequent known types must equal this known type
                    if (inputTypeClass == null) {
                        continue;
                    }
                    if (pickSameType == null)
                        pickSameType = new TClass[overload.inputSetIndexCount()];
                    int inputSetIndex = overload.inputSetIndexAtPos(i);
                    TClass definedType = pickSameType[inputSetIndex];
                    if (definedType == null) {
                        pickSameType[inputSetIndex] = inputTypeClass;
                        continue;
                    }
                    else if (definedType == inputTypeClass) {
                        continue;
                    }
                }
                else if (inputTypeClass == null && scalarGroups.hasSameTypeAt(i)) {
                    continue;
                }
                else if (inputTypeClass == inputSet.targetType()) {
                    continue;
                }
            }
            else {
                if (castsResolver.strongCastExists(inputTypeClass, inputSet.targetType()))
                    continue;
            }
            // This input precludes the use of the overload
            return false;
        }
        // no inputs have precluded this overload
        return true;
    }

    private OverloadResult<V> buildResult(V overload, List<? extends TPreptimeValue> inputs)
    {
        return new OverloadResult<>(overload, inputs, castsResolver);
    }

    /*
     * Two overloads have SIMILAR INPUT SETS if they
     *   1) have the same number of input sets
     *   2) each input set from one overload covers the same columns as an input set from the other function
     *
     * For any two overloads A and B, if A and B have SIMILAR INPUT SETS, and the target type of each input
     * set Ai can be strongly cast to the target type of Bi, then A is said to be MORE SPECIFIC than A, and B
     * is discarded as a possible overload.
     */
    private List<List<V>> reduceToMinimalCastGroups(List<V> candidates) {
        // NOTE:
        // This method shares some concepts with #commonTClass. See that method for a note about possible refactoring
        // opportunities (tl;dr is the two methods don't share code right now, but they might be able to.)
        List<List<V>> castGroups = new ArrayList<>();

        for(V B : candidates) {
            final int nInputSets = B.inputSets().size();

            // Find the OVERLOAD CAST GROUP
            List<V> castGroup = null;
            for(List<V> group : castGroups) {
                // Groups are not empty, can always get first
                V cur = group.get(0);
                if(cur.inputSets().size() == nInputSets) {
                    boolean matches = true;
                    for(int i = 0; i < nInputSets && matches; ++i) {
                        matches = (cur.inputSetAt(i).positionsLength() == B.inputSetAt(i).positionsLength());
                    }
                    if(matches) {
                        castGroup = group;
                        break;
                    }
                }
            }

            if(castGroup != null) {
                // Found group, check for more specific
                Iterator<V> it = castGroup.iterator();
                while(it.hasNext()) {
                    V A = it.next();
                    boolean AtoB = true;
                    boolean BtoA = true;
                    for(int i = 0; i < nInputSets; ++i) {
                        TInputSet Ai = A.inputSetAt(i);
                        TInputSet Bi = B.inputSetAt(i);
                        AtoB &= castsResolver.strongCastExists(Ai.targetType(), Bi.targetType());
                        BtoA &= castsResolver.strongCastExists(Bi.targetType(), Ai.targetType());
                    }
                    if(AtoB) {
                        // current more specific
                        // set B to null so that we know not to add it to castGroup
                        B = null;
                        break;
                    } else if(BtoA) {
                        // new more specific
                        it.remove();
                    }
                }
                if(B != null) {
                    // No more specific existed or B was most specific
                    castGroup.add(B);
                }
            } else {
                // No matching group, must be in a new group
                castGroup = new ArrayList<>(1);
                castGroup.add(B);
                castGroups.add(castGroup);
            }
        }
        return castGroups;
    }


}
