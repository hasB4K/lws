import test from 'node:test';
import assert from 'node:assert/strict';
import {exportDuration, gifSchedule, videoSupport, videoBoundaries, videoState} from '../assets/js/trace-export.ts';
const drawing = durations => ({width:1920,height:1080,frames:durations.map((duration,i)=>({duration,items:[i]}))});

test('static GIF preserves long holds without repeated frames, including normal-speed harness', () => {
  const data=drawing([10,200,17]);
  assert.deepEqual(gifSchedule(data,1),[{time:0,delay:10000},{time:10,delay:200000},{time:210,delay:17000}]);
  assert.equal(exportDuration(data,2),113.5);
});
test('GIF coalesces only identical adjacent visuals and preserves changed states', () => {
  const data=drawing([1,2,3]); data.frames[1].items=[0];
  assert.deepEqual(gifSchedule(data,1),[{time:0,delay:3000},{time:3,delay:3000}]);
});
test('motion sampling includes every state boundary and exact centisecond total', () => {
  const data=drawing([.13,.31,1]);data.motions=[{}];
  const frames=gifSchedule(data,2);
  assert.ok(frames.some(frame=>frame.time===.13));assert.ok(frames.some(frame=>frame.time===.44));
  assert.equal(frames.reduce((sum,frame)=>sum+frame.delay,0),720);
});
test('rejects excessive duration, frame count, invalid speed, or unrepresentable holds', () => {
  assert.throws(()=>gifSchedule(drawing([601]),1),/10 minutes/);
  assert.throws(()=>gifSchedule(drawing(Array(1201).fill(.1)),1),/1,200/);
  assert.throws(()=>gifSchedule({...drawing([121]),motions:[{}]},1),/1,200/);
  assert.throws(()=>gifSchedule(drawing([.001]),1),/minimum/);
  assert.throws(()=>gifSchedule(drawing([.01]),1),/minimum/);
  for(const speed of [NaN,0,-1,17]) assert.throws(()=>exportDuration(drawing([1]),speed),/speed/);
});
test('video preflight rejects sub-frame states; runtime refuses skipped evidence after jank', () => {
  assert.throws(()=>videoBoundaries(drawing([.11,.01,.09]),1),/24 fps/);
  assert.deepEqual(videoBoundaries(drawing([.11,.01,.09]),.1),[0,1.0999999999999999,1.2]);
  assert.equal(videoState([0,.1,.2],.11,0),1);
  assert.throws(()=>videoState([0,.1,.2],.21,0),/skipped a visual state/);
  assert.equal(videoState([0,.1,.2],.21,1),2);
});
test('native video reports MP4, accurately labeled WebM fallback, or unavailable', () => {
  const previous={MediaRecorder:globalThis.MediaRecorder,HTMLCanvasElement:globalThis.HTMLCanvasElement};
  try {
    globalThis.HTMLCanvasElement=class{captureStream(){}};
    globalThis.MediaRecorder={isTypeSupported:type=>type.startsWith('video/mp4')};
    assert.equal(videoSupport().extension,'mp4');
    globalThis.MediaRecorder={isTypeSupported:type=>type.startsWith('video/webm')};
    assert.equal(videoSupport().extension,'webm');
    globalThis.MediaRecorder={isTypeSupported:()=>false};assert.equal(videoSupport(),null);
  } finally {
    for(const [key,value] of Object.entries(previous)) if(value===undefined) delete globalThis[key];else globalThis[key]=value;
  }
});
