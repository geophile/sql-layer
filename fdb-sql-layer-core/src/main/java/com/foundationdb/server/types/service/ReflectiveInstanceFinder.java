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

import com.foundationdb.server.error.AkibanInternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;


public class ReflectiveInstanceFinder implements InstanceFinder
{
    private static final Logger logger = LoggerFactory.getLogger(ReflectiveInstanceFinder.class);

    private final Set<Class<?>> searchClasses;
    
    private static final int SKIP = -1;
    private static final int FIELD = 0;
    private static final int ARRAY = 1;
    private static final int COLLECTION = 2;

    public ReflectiveInstanceFinder()
    throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        this(new ConfigurableClassFinder("typedirs.txt"));
    }

    public ReflectiveInstanceFinder(ClassFinder classFinder)
    throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        // collect all scalar TOverload instances
        searchClasses = classFinder.findClasses();
    }

    @Override
    public <T> Collection<? extends T> find(Class<? extends T> targetClass) {
        return collectInstances(searchClasses, targetClass);
    }

    private static <T> Collection<T> collectInstances(Collection<Class<?>> classes, Class<T> target)
    {
        List<T> ret = new ArrayList<>();
        for (Class<?> cls : classes) {
            if (!Modifier.isPublic(cls.getModifiers()))
                continue;
            else
                doCollecting(ret, cls, target);
        }
        return ret;
    }

    private static <T> void doCollecting(Collection<T> ret, Class<?> cls, Class<T> target) 
    {
        try
        {
            // grab the static INSTANCEs fields
            for (Field field : cls.getFields())
            {
                if (isRegistered(field))
                    switch(validateField(field, target))
                    {
                        case FIELD:
                            putItem(ret, field.get(null), target, field);
                            break;
                        case ARRAY:
                            for (Object item : (Object[])field.get(null)) {
                                if (target.isInstance(item))
                                    putItem(ret, item, target, field);
                            }
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)field.get(null)) {
                                    if (target.isInstance(raw))
                                        putItem(ret, raw, target, field);
                                }
                                break;
                            }
                            catch (ClassCastException e) {/* fall thru */}
                        default:
                               // SKIP (does nothing)
                    }
            }
            
            // grab the static methods that create instances
            for (Method method : cls.getMethods())
            {
                
                if (isRegistered(method))
                    switch(validateMethod(method, target))
                    {
                        case FIELD:
                            putItem(ret, method.invoke(null), target, method);
                            break;
                        case ARRAY:
                            for (Object item : (Object[])method.invoke(null))
                                putItem(ret, item, target, method);
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)method.invoke(null))
                                    putItem(ret, raw, target, method);
                                break;
                            }
                            catch (ClassCastException e) {/* fall thru */}
                        default:
                            // SKIP (does nothing)
                    }
            }
           
        }
        catch (IllegalAccessException e)   
        {
            throw new FunctionsRegistryException(e.getMessage());
        }
        catch (InvocationTargetException ex)
        {
            throw new FunctionsRegistryException(ex.getMessage());
        }
    }

    public static class FunctionsRegistryException extends AkibanInternalException {
        public FunctionsRegistryException(String message) {
            super(message);
        }
    }

    private static <T> int validateMethod(Method method, Class<T> target)
    {
        int modifiers = method.getModifiers();
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers)
                && method.getParameterTypes().length == 0)
            return assignable(method.getReturnType(), target, method.getGenericReturnType());
        return SKIP;
    }
    
    private static <T> int validateField(Field field, Class<T> target)
    { 
        int modifiers = field.getModifiers();
        
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers))
            return assignable(field.getType(), target, field.getGenericType());
         return SKIP;
    }

    private static <T> int assignable (Class<?> c, Class<T> target, Type genericTarget)
    {
        if (c.isArray() && target.isAssignableFrom(c.getComponentType())) {
            return ARRAY;
        }
        else if (target.isAssignableFrom(c)) {
            return FIELD;
        }
        else if (Collection.class.isAssignableFrom(c)) {
            if (genericTarget instanceof ParameterizedType) {
                ParameterizedType targetParams = (ParameterizedType) genericTarget;
                Type[] genericArgs = targetParams.getActualTypeArguments();
                assert genericArgs.length == 1 : Arrays.toString(genericArgs);
                Type genericArg = genericArgs[0];
                if (genericArg instanceof WildcardType) {
                    Type[] upperBounds = ((WildcardType)genericArg).getUpperBounds();
                    if (upperBounds.length > 1)
                        logger.debug("multiple upper bounds for {}: {}", genericTarget, Arrays.toString(upperBounds));
                    for (Type upperBound : upperBounds) {
                        if (isAssignableFrom(target, upperBound))
                            return COLLECTION;
                    }
                }
                else if (isAssignableFrom(target, genericArg))
                    return COLLECTION;
            }
        }
        return SKIP;
    }

    private static boolean isAssignableFrom(Class<?> target, Type actualType) {
        return (actualType instanceof Class<?>) && target.isAssignableFrom((Class<?>) actualType);
    }

    private static <T> void putItem(Collection<T> list, Object item,  Class<T> targetClass, Object source)
    {
        T cast;
        try {
            cast = targetClass.cast(item);
        } catch (ClassCastException e) {
            String err = "while casting " + item + " from " + source + " to " + targetClass;
            logger.error(err, e);
            throw new ClassCastException(err);
        }
        list.add(cast);
    }

    private static boolean isRegistered(AccessibleObject field)
    {
        return field.getAnnotation(DontRegister.class) == null;
    }
}
