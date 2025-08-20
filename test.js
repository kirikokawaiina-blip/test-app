(function() {
    const els = {
        syncUrl: document.getElementById('syncUrl'),
        syncRoom: document.getElementById('syncRoom'),
        syncKey: document.getElementById('syncKey'),
        connectBtn: document.getElementById('connectBtn'),
        status: document.getElementById('status'),
        serverState: document.getElementById('server-state'),
        usersInfo: document.getElementById('users-info'),

        // Impersonation UI elements
        userSelect: document.getElementById('user-select'),
        opSelect: document.getElementById('op-select'),
        dynamicFormContainer: document.getElementById('dynamic-form-container'),
        sendOpBtn: document.getElementById('send-op-btn'),
        opStatus: document.getElementById('op-status'),
    };

    const SCHEMAS = {
        '': [], // Default empty state
        transfer: [
            { name: 'toId', type: 'user_id', label: '送信先ユーザー' },
            { name: 'amount', type: 'number', label: '金額' },
            { name: 'memo', type: 'string', label: 'メモ (任意)', optional: true },
        ],
        create_listing: [
            { name: 'title', type: 'string', label: 'タイトル' },
            { name: 'price', type: 'number', label: '価格' },
            { name: 'desc', type: 'string', label: '説明 (任意)', optional: true },
            { name: 'qty', type: 'number', label: '在庫', optional: true },
        ],
        buy_listing: [
            { name: 'listingId', type: 'listing_id', label: '出品ID' },
        ],
        delete_listing: [
            { name: 'listingId', type: 'listing_id', label: '出品ID' },
        ],
        morning_claim: [], // No parameters needed
        roulette: [
            // The server uses its own random roll, but we can send one for compatibility
            { name: 'roll', type: 'number', label: 'クライアント側Roll (任意)', optional: true, default: Math.random() },
        ],
        buyer_request: [
            { name: 'rightId', type: 'right_id', label: '権利ID' },
        ],
        seller_respond: [
            { name: 'rightId', type: 'right_id', label: '権利ID' },
            { name: 'action', type: 'string', label: 'アクション (exec or cancel)' },
        ],
        buyer_finalize: [
            { name: 'rightId', type: 'right_id', label: '権利ID' },
        ]
    };

    let syncInterval = null;
    let lastSyncTime = 0;
    let serverState = {};

    function loadSettings() {
        const settings = JSON.parse(localStorage.getItem('trpg_sync_kv_test_page') || '{}');
        els.syncUrl.value = settings.url || '';
        els.syncRoom.value = settings.room || '';
        els.syncKey.value = settings.key || '';
    }

    function saveSettings() {
        const settings = {
            url: els.syncUrl.value.trim(),
            room: els.syncRoom.value.trim(),
            key: els.syncKey.value.trim(),
        };
        localStorage.setItem('trpg_sync_kv_test_page', JSON.stringify(settings));
        return settings;
    }

    function populateUserDropdown(users) {
        const currentVal = els.userSelect.value;
        els.userSelect.innerHTML = '';
        users.forEach(user => {
            const option = document.createElement('option');
            option.value = user.id;
            option.textContent = `${user.name} (ID: ${user.id.substring(0, 5)}...)`;
            els.userSelect.appendChild(option);
        });
        if (currentVal) els.userSelect.value = currentVal;
    }

    function generateDynamicForm() {
        const opName = els.opSelect.value;
        const params = SCHEMAS[opName] || [];
        const container = els.dynamicFormContainer;
        container.innerHTML = '';

        if (params.length === 0 && opName) {
            container.textContent = 'この操作にはパラメータは不要です。';
        }

        const formGrid = document.createElement('div');
        formGrid.style.display = 'grid';
        formGrid.style.gridTemplateColumns = 'auto 1fr';
        formGrid.style.gap = '10px';
        formGrid.style.alignItems = 'center';

        params.forEach(param => {
            const label = document.createElement('label');
            label.htmlFor = `param-${param.name}`;
            label.textContent = `${param.label}:`;

            let input;
            if (param.type === 'user_id' || param.type === 'listing_id' || param.type === 'right_id') {
                input = document.createElement('select');
                const source = param.type === 'user_id' ? serverState.users : (param.type === 'listing_id' ? serverState.listings : serverState.rights);
                if (source) {
                    source.forEach(item => {
                        const option = document.createElement('option');
                        option.value = item.id;
                        option.textContent = `${item.title || item.name} (ID: ${item.id.substring(0, 5)}...)`;
                        input.appendChild(option);
                    });
                }
            } else {
                input = document.createElement('input');
                input.type = param.type;
                if (param.default) input.value = param.default;
            }

            input.id = `param-${param.name}`;
            input.dataset.paramName = param.name;

            formGrid.appendChild(label);
            formGrid.appendChild(input);
        });
        container.appendChild(formGrid);
    }

    async function fetchServerState() {
        const settings = saveSettings();
        if (!settings.url || !settings.room || !settings.key) {
            els.status.textContent = 'ステータス: 設定が不完全です';
            return;
        }
        els.status.textContent = 'ステータス: データを取得中...';

        try {
            const url = new URL(settings.url);
            url.searchParams.set('room', settings.room);
            url.searchParams.set('key', settings.key);
            url.searchParams.set('lastSync', lastSyncTime);

            const response = await fetch(url.toString());
            if (response.status === 204) {
                els.status.textContent = `ステータス: サーバーに変更なし (${new Date().toLocaleTimeString()})`;
                return;
            }
            if (!response.ok) throw new Error(`サーバーエラー: ${response.status}`);

            const data = await response.json();
            serverState = data.state || {};

            els.serverState.textContent = JSON.stringify(data, null, 2);

            if (serverState.users) {
                populateUserDropdown(serverState.users);
                const usersText = serverState.users.map(u => `ID: ${u.id}\n名前: ${u.name}\n残高: ${u.balance}`).join('\n\n');
                els.usersInfo.textContent = usersText;
            } else {
                els.usersInfo.textContent = 'ユーザー情報が見つかりません。';
            }

            lastSyncTime = data.lastUpdate || new Date().getTime();
            els.status.textContent = `ステータス: 接続中 (${new Date().toLocaleTimeString()}に更新)`;

            // Re-generate form in case dropdowns need updating
            generateDynamicForm();

        } catch (error) {
            els.status.textContent = `ステータス: エラー - ${error.message}`;
            console.error('Fetch error:', error);
            if (syncInterval) {
                clearInterval(syncInterval);
                syncInterval = null;
                els.connectBtn.textContent = '接続';
            }
        }
    }

    async function sendImpersonatedOperation() {
        const settings = saveSettings();
        const userId = els.userSelect.value;
        const opType = els.opSelect.value;

        if (!userId || !opType) {
            els.opStatus.textContent = 'ユーザーと操作を選択してください。';
            return;
        }

        const opData = {};
        const params = SCHEMAS[opType] || [];
        for (const param of params) {
            const input = document.getElementById(`param-${param.name}`);
            if (input) {
                let value = input.value;
                if (param.type === 'number') value = Number(value);
                if (!param.optional || value) {
                    opData[param.name] = value;
                }
            } else if (!param.optional) {
                els.opStatus.textContent = `必須パラメータ '${param.name}' が見つかりません。`;
                return;
            }
        }

        const operation = {
            id: 'op_' + Math.random().toString(36).slice(2) + '_' + Date.now(),
            type: opType,
            data: opData,
            timestamp: Date.now(),
            userId: userId,
            clientId: 'client_' + Math.random().toString(36).slice(2),
            sessionId: 'session_' + Math.random().toString(36).slice(2),
        };

        els.opStatus.textContent = '操作を送信中...';

        try {
            const url = new URL(settings.url);
            url.searchParams.set('room', settings.room);
            url.searchParams.set('key', settings.key);

            const response = await fetch(url.toString(), {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ operations: [operation] }),
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`サーバーエラー: ${response.status} - ${errorText}`);
            }

            const result = await response.json();
            els.opStatus.textContent = `送信成功！ (${new Date().toLocaleTimeString()})`;
            console.log('Operation result:', result);

            lastSyncTime = 0;
            await fetchServerState();

        } catch (error) {
            els.opStatus.textContent = `送信エラー: ${error.message}`;
            console.error('Send operation error:', error);
        }
    }

    function init() {
        loadSettings();

        Object.keys(SCHEMAS).forEach(opName => {
            const option = document.createElement('option');
            option.value = opName;
            option.textContent = opName || '選択してください...';
            els.opSelect.appendChild(option);
        });

        els.opSelect.addEventListener('change', generateDynamicForm);
        els.connectBtn.addEventListener('click', () => {
            if (syncInterval) {
                clearInterval(syncInterval);
                syncInterval = null;
                els.status.textContent = 'ステータス: 未接続';
                els.connectBtn.textContent = '接続';
            } else {
                lastSyncTime = 0;
                fetchServerState();
                syncInterval = setInterval(fetchServerState, 3000);
                els.connectBtn.textContent = '停止';
            }
        });
        els.sendOpBtn.addEventListener('click', sendImpersonatedOperation);
    }

    init();
})();
