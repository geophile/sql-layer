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
package com.foundationdb.server.explain;

import java.util.*;

public class Attributes extends EnumMap<Label, List<Explainer>>
{       
    public Attributes()
    {
        super(Label.class);
    }            
    
    public Explainer getAttribute(Label label) {
        List<Explainer> l = get(label);
        if (l == null)
            return null;
        assert (l.size() == 1) : l;
        return l.get(0);
    }

    public Object getValue(Label label) {
        Explainer explainer = getAttribute(label);
        if (explainer == null)
            return null;
        return explainer.get();
    }

    public boolean put(Label label, Explainer ex)
    {
        List<Explainer> l = get(label);
        if (l == null)
        {
            l = new ArrayList<>();
            put(label, l);
        }
        l.add(ex);
        return true;
    }
        
    public List<Entry<Label, Explainer>> valuePairs()
    {
        List<Entry<Label, Explainer>> pairs = new ArrayList<>();
        
        for (Entry<Label, List<Explainer>> entry : entrySet())                
            for (Explainer ex : entry.getValue())            
                pairs.add(new ValuePair(entry.getKey(), ex, this));                            
        return pairs;
    }
    
    private static class ValuePair implements Entry<Label, Explainer>
    {
        private Label key;
        private Explainer value;
        private Attributes map;
        
        protected ValuePair (Label k, Explainer v, Attributes m)
        {
            key = k;
            value = v;
            map = m;
        }
        
        @Override
        public Label getKey()
        {
            return key;
        }

        @Override
        public Explainer getValue()
        {
            return value;
        }

        @Override
        public Explainer setValue(Explainer value)
        {
            Explainer old = this.value;            
            List<Explainer> s = map.get(key);
            this.value = value;
            s.remove(old);
            s.add(value);      
            return old;
        }
        
    }
}
