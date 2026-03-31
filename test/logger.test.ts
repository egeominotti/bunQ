/**
 * Logger Tests
 * LOG_LEVEL filtering and structured logging
 */

import { describe, test, expect, beforeEach, afterEach, spyOn } from 'bun:test';
import { Logger, createLogger } from '../src/shared/logger';

describe('Logger', () => {
  let consoleLogSpy: ReturnType<typeof spyOn>;
  let consoleDebugSpy: ReturnType<typeof spyOn>;
  let consoleWarnSpy: ReturnType<typeof spyOn>;
  let consoleErrorSpy: ReturnType<typeof spyOn>;

  beforeEach(() => {
    consoleLogSpy = spyOn(console, 'log').mockImplementation(() => {});
    consoleDebugSpy = spyOn(console, 'debug').mockImplementation(() => {});
    consoleWarnSpy = spyOn(console, 'warn').mockImplementation(() => {});
    consoleErrorSpy = spyOn(console, 'error').mockImplementation(() => {});
    Logger.disableJsonMode();
    Logger.setLevel('debug');
  });

  afterEach(() => {
    consoleLogSpy.mockRestore();
    consoleDebugSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
    Logger.setLevel('info');
  });

  describe('human-readable mode', () => {
    test('should log info messages', () => {
      const logger = new Logger('TestComponent');
      logger.info('Test message');

      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain('[TestComponent]');
      expect(call).toContain('Test message');
    });

    test('should log debug messages', () => {
      const logger = new Logger('TestComponent');
      logger.debug('Debug message');

      expect(consoleDebugSpy).toHaveBeenCalled();
    });

    test('should log warn messages', () => {
      const logger = new Logger('TestComponent');
      logger.warn('Warning message');

      expect(consoleWarnSpy).toHaveBeenCalled();
    });

    test('should log error messages', () => {
      const logger = new Logger('TestComponent');
      logger.error('Error message');

      expect(consoleErrorSpy).toHaveBeenCalled();
    });

    test('should include data in log', () => {
      const logger = new Logger('TestComponent');
      logger.info('Test message', { key: 'value' });

      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain('{"key":"value"}');
    });
  });

  describe('JSON mode', () => {
    beforeEach(() => {
      Logger.enableJsonMode();
    });

    test('should output valid JSON', () => {
      const logger = new Logger('TestComponent');
      logger.info('Test message');

      expect(consoleLogSpy).toHaveBeenCalled();
      const output = consoleLogSpy.mock.calls[0][0];
      const parsed = JSON.parse(output);

      expect(parsed.component).toBe('TestComponent');
      expect(parsed.message).toBe('Test message');
      expect(parsed.level).toBe('info');
      expect(parsed.timestamp).toBeDefined();
    });

    test('should include timestamp in ISO format', () => {
      const logger = new Logger('TestComponent');
      logger.info('Test');

      const output = consoleLogSpy.mock.calls[0][0];
      const parsed = JSON.parse(output);

      // Should be valid ISO timestamp
      const date = new Date(parsed.timestamp);
      expect(date.toISOString()).toBe(parsed.timestamp);
    });

    test('should include data in JSON output', () => {
      const logger = new Logger('TestComponent');
      logger.info('Test', { userId: 123, action: 'login' });

      const output = consoleLogSpy.mock.calls[0][0];
      const parsed = JSON.parse(output);

      expect(parsed.data.userId).toBe(123);
      expect(parsed.data.action).toBe('login');
    });

    test('should log all levels as JSON', () => {
      const logger = new Logger('Test');

      logger.debug('debug msg');
      logger.info('info msg');
      logger.warn('warn msg');
      logger.error('error msg');

      // All should go to console.log in JSON mode
      expect(consoleLogSpy).toHaveBeenCalledTimes(4);

      const levels = consoleLogSpy.mock.calls.map((call) => JSON.parse(call[0]).level);
      expect(levels).toEqual(['debug', 'info', 'warn', 'error']);
    });
  });

  describe('createLogger', () => {
    test('should create logger with component name', () => {
      const logger = createLogger('MyModule');
      logger.info('Test');

      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain('[MyModule]');
    });
  });

  describe('mode switching', () => {
    test('should switch between modes', () => {
      const logger = new Logger('Test');

      // Human mode
      Logger.disableJsonMode();
      logger.info('human');
      let call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain('[Test]');

      // JSON mode
      Logger.enableJsonMode();
      logger.info('json');
      call = consoleLogSpy.mock.calls[1][0];
      const parsed = JSON.parse(call);
      expect(parsed.component).toBe('Test');
    });
  });

  describe('LOG_LEVEL filtering', () => {
    test('should filter debug messages when level is info', () => {
      Logger.setLevel('info');
      const log = createLogger('Test');

      log.debug('should be filtered');
      expect(consoleDebugSpy).not.toHaveBeenCalled();
    });

    test('should allow info messages when level is info', () => {
      Logger.setLevel('info');
      const log = createLogger('Test');

      log.info('should pass');
      expect(consoleLogSpy).toHaveBeenCalled();
    });

    test('should filter info and debug when level is warn', () => {
      Logger.setLevel('warn');
      const log = createLogger('Test');

      log.debug('filtered');
      log.info('filtered');
      log.warn('passes');

      expect(consoleDebugSpy).not.toHaveBeenCalled();
      expect(consoleLogSpy).not.toHaveBeenCalled();
      expect(consoleWarnSpy).toHaveBeenCalled();
    });

    test('should only allow error when level is error', () => {
      Logger.setLevel('error');
      const log = createLogger('Test');

      log.debug('filtered');
      log.info('filtered');
      log.warn('filtered');
      log.error('passes');

      expect(consoleDebugSpy).not.toHaveBeenCalled();
      expect(consoleLogSpy).not.toHaveBeenCalled();
      expect(consoleWarnSpy).not.toHaveBeenCalled();
      expect(consoleErrorSpy).toHaveBeenCalled();
    });

    test('should allow all messages when level is debug', () => {
      Logger.setLevel('debug');
      const log = createLogger('Test');

      log.debug('passes');
      log.info('passes');

      expect(consoleDebugSpy).toHaveBeenCalled();
      expect(consoleLogSpy).toHaveBeenCalled();
    });

    test('should work with JSON mode and level filtering', () => {
      Logger.enableJsonMode();
      Logger.setLevel('warn');
      const log = createLogger('Test');

      log.info('should be filtered even in JSON mode');
      expect(consoleLogSpy).not.toHaveBeenCalled();

      log.warn('should pass in JSON mode');
      expect(consoleLogSpy).toHaveBeenCalled();
    });
  });

  describe('setLevel', () => {
    test('should accept all valid levels', () => {
      expect(() => Logger.setLevel('debug')).not.toThrow();
      expect(() => Logger.setLevel('info')).not.toThrow();
      expect(() => Logger.setLevel('warn')).not.toThrow();
      expect(() => Logger.setLevel('error')).not.toThrow();
    });
  });
});
