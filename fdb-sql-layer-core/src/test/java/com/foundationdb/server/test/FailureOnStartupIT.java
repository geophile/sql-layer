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
package com.foundationdb.server.test;

import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.sql.Main;
import com.google.inject.Inject;
import org.junit.Test;
import java.util.Map;

import static org.junit.Assert.assertFalse;

public final class FailureOnStartupIT extends ApiTestBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bind(LayerInfoInterface.class, BadLayerInfoInterface.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void one() {
        // failure would have happened in @Before
        assertFalse(stage == Stage.FIRST_START);
    }

    @Test
    public void two() {
        // failure would have happened in @Before, but hopefully not at all!
        // we can't assert that stage is SUBSEQUENT, because JUnit doesn't guarantee ordering
        assertFalse(stage == Stage.FIRST_START);
    }

    public FailureOnStartupIT() {
        super("IT");
    }

    @Override
    void handleStartupFailure(Exception e) throws Exception {
        // eat only the first failure
        if (stage == Stage.FIRST_FAILURE) {
            stage = Stage.SUBSEQUENT;
            return;
        }
        throw e;
    }

    private static Stage stage = Stage.FIRST_START;

    private enum Stage {
        FIRST_START,
        FIRST_FAILURE,
        SUBSEQUENT
    }

    public static class BadLayerInfoInterface extends Main
    {
        @Inject
        public BadLayerInfoInterface(ConfigurationService config) {
            super(config);
        }

        @Override
        public void start() {
            if (stage == Stage.FIRST_START) {
                stage = Stage.FIRST_FAILURE;
                throw new RuntimeException();
            }
            super.start();
        }
    }
}
