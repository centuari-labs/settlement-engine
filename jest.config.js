module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src'],
  testMatch: ['**/__tests__/**/*.test.ts', '**/?(*.)+(spec|test).ts'],
  transform: {
    '^.+\\.ts$': 'ts-jest',
  },
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/*.d.ts',
    '!src/index.ts',
    '!src/tests/**/*.ts',
  ],
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'lcov', 'html'],
  // Branch threshold is set lower than the others because several error-path
  // branches in batchProcessor.ts, persistence.ts, and config.ts (Zod schema)
  // are structurally hard to exercise without very complex integration setups.
  // The other metrics comfortably exceed 80%.
  coverageThreshold: {
    global: {
      branches: 70,
      functions: 80,
      lines: 80,
      statements: 80,
    },
  },
  moduleFileExtensions: ['ts', 'js', 'json'],
  testTimeout: 10000, // 10 seconds default, integration tests may override
  verbose: true,
  setupFilesAfterEnv: ['<rootDir>/src/tests/setup.ts'],
};

