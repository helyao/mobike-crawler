
divided = [
    [[121.155127, 31.51821], [121.340493, 31.402815]],
    [[121.155127, 31.402815], [121.340493, 31.28742]],
    [[121.069133, 31.28742], [121.204813, 31.145111]],
    [[121.204813, 31.28742], [121.340493, 31.145111]],
    [[120.866763, 31.145111], [121.024673, 31.018433]],
    [[121.024673, 31.145111], [121.182583, 31.018433]],
    [[121.182583, 31.145111], [121.340493, 31.018433]],
    [[121.000143, 31.018433], [121.138123, 30.855878]],
    [[121.138123, 31.018433], [121.340493, 30.90997]],
    [[121.138123, 30.90997], [121.340493, 30.801507]],
    [[121.138123, 30.801507], [121.340493, 30.693043]],
    [[121.340493, 31.51821], [121.406236, 31.243155]],
    [[121.340493, 31.243155], [121.406236, 30.9681]],
    [[121.340493, 30.9681], [121.406236, 30.693043]],
    [[121.406236, 31.483357], [121.476236, 31.238629]],
    [[121.406236, 31.238629], [121.476236, 30.993901]],
    [[121.406236, 30.993901], [121.476236, 30.749173]],
    [[121.476236, 31.43617], [121.576236, 31.220525]],
    [[121.476236, 31.220525], [121.576236, 31.00488]],
    [[121.476236, 31.00488], [121.576236, 30.789235]],
    [[121.576236, 31.381281], [121.696236, 31.195843]],
    [[121.576236, 31.195843], [121.696236, 31.010405]],
    [[121.576236, 31.010405], [121.696236, 30.824967]],
    [[121.696236, 31.320529], [121.78736, 31.086608]],
    [[121.696236, 31.086608], [121.78736, 30.852687]],
    [[121.78736, 31.217113], [121.901585, 31.0349]],
    [[121.78736, 31.0349], [121.901585, 30.852687]],
    [[121.901585, 31.078579], [121.983144, 30.853756]]
]

for item in range(0, len(divided)):
    left = divided[item][0][0]
    top = divided[item][0][1]
    right = divided[item][1][0]
    bottom = divided[item][1][1]
    print('[Block{}]'.format(item+1))
    print('left={}'.format(left))
    print('right={}'.format(right))
    print('top={}'.format(top))
    print('bottom={}'.format(bottom))
